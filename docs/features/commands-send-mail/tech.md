# Commands — SendMail (technical)

> Companion to [`spec.md`](./spec.md). Wire layout, handler walkthrough,
> error-code mapping, threading.

## Wire shape

Inbound record on `<tenant>.gs.commands`:

```
key:   longBytesBe(charId)
headers:
  Nx-Server-Id      = raw 16-byte tenant/server uuid (auto-stamped post-/connect)
  Nx-Message-Type   = "SendMailCommand"
  Nx-Correlation-Id = <platform-issued UUID>
value: gson(SendMailCommand JSON)
```

Reply record on `<tenant>.gs.commands.replies`:

```
key:   longBytesBe(corrId.getMostSignificantBits())
headers:
  Nx-Server-Id      = raw 16-byte tenant/server uuid
  Nx-Message-Type   = "SendMailCommandResult"
  Nx-Correlation-Id = <inbound corrId>
value: gson(CommandResult<SendMailPayload> JSON)
```

Example value (success with batched mails + one partial failure):

```json
{
  "success": true,
  "errorDetails": {},
  "payload": {
    "createdMailIds": [10241, 10242],
    "itemErrors": [
      {
        "itemTemplateId": null,
        "count": null,
        "reason": "Failed to create mail attachment itemId: 99999, count: 1 for charId: 42"
      }
    ]
  }
}
```

Identity fields are `null` today because `MailManager` does not expose
machine-readable per-line failure identity (see "Mapping `itemErrors`" below).
The platform inspects `reason` for diagnostic context; the schema is wire-additive
so a future MailManager refactor can populate `itemTemplateId` / `count` without
an api version bump.

Example value (recipient not found):

```json
{
  "success": false,
  "errorCode": "NOT_FOUND",
  "errorDetails": {"entity": "character"}
}
```

## Field reference

| Field                    | Type          | Required | Notes                                            |
|--------------------------|---------------|----------|--------------------------------------------------|
| `charId`                 | `Long`        | Yes      | Recipient character pk                           |
| `author`                 | `String?`     | No       | Sender display; null/blank → host system default |
| `title`                  | `String`      | Yes      | Mail subject; MailManager rejects blank          |
| `body`                   | `String?`     | No       | Mail body; null → empty                          |
| `items`                  | `[MailItem]?` | No       | Attachments; null/empty → text-only mail         |
| `items[].itemTemplateId` | `Long`        | Yes      | Catalog template id (NOT object-id)              |
| `items[].count`          | `Long`        | Yes      | Stack size, positive                             |

Reply payload:

| Field                         | Type                  | Notes                                                        |
|-------------------------------|-----------------------|--------------------------------------------------------------|
| `createdMailIds`              | `[Long]`              | One entry per resulting mail (host-side batching)            |
| `itemErrors`                  | `[ItemDeliveryError]` | Per-line failures; non-empty + success = partial delivery    |
| `itemErrors[].itemTemplateId` | `Long?`               | Optional inbound-line hint; `null` when host can't attribute |
| `itemErrors[].count`          | `Long?`               | Optional inbound-line hint; `null` when host can't attribute |
| `itemErrors[].reason`         | `String`              | Free-form diagnostic; null normalized to ""                  |

## Error-code mapping

| Condition                                       | ErrorCode           | errorDetails                                               |
|-------------------------------------------------|---------------------|------------------------------------------------------------|
| `charId` null on wire                           | `VALIDATION_FAILED` | `{field: "charId"}`                                        |
| `title` null on wire                            | `VALIDATION_FAILED` | `{field: "title"}`                                         |
| `items[i]` null                                 | `VALIDATION_FAILED` | `{items[<i>]: "null"}`                                     |
| `items[i].itemTemplateId` null                  | `VALIDATION_FAILED` | `{items[<i>].itemTemplateId: "null"}`                      |
| `items[i].itemTemplateId` > `Integer.MAX_VALUE` | `VALIDATION_FAILED` | `{items[<i>].itemTemplateId: "exceeds-int-range:<value>"}` |
| `items[i].count` null or ≤ 0                    | `VALIDATION_FAILED` | `{items[<i>].count: "<value>"}`                            |
| `MailManager` throws `IllegalArgumentException` | `VALIDATION_FAILED` | `{mailManager: "<message>"}`                               |
| `CharacterDAO.getByCharId` empty                | `NOT_FOUND`         | `{entity: "character"}`                                    |
| `MailManager` returns empty list (invariant)    | `INTERNAL_ERROR`    | `{error.cause: "mailManager-returned-empty"}`              |
| Other exceptions in handler                     | `INTERNAL_ERROR`    | adapter auto-wraps with class+message                      |
| Per-attachment-line failure                     | (success envelope)  | `payload.itemErrors[].reason`                              |

## Handler walkthrough

`l2e.gameserver.l2nx.commands.handlers.SendMailHandler.handle(cmd, ctx)`:

1. **Wire-validate** `charId` / `title` non-null (Gson bypasses ctor).
2. **Wire-validate** every `MailItem`: non-null entry, non-null `itemTemplateId`,
   non-null + positive `count`. Index-tagged details so the platform can pinpoint
   the bad line.
3. **DAO lookup** `CharacterDAO.getByCharId(cmd.getCharId().intValue())` — runs
   on the consumer thread deliberately (early-fail on stale charId without
   burning a host-pool hop). The `CommandHandler` SPI permits read-only DB
   I/O on the consumer thread. There is a TOCTOU window — the recipient could
   be deleted between the lookup and the host-thread hop — but the resulting
   orphan mail row is recoverable and the legacy RabbitMQ path
   (`MailService.sendMailAndReply`) has the same race. Future maintainers
   should NOT move this lookup inside the hop "for symmetry with
   `DeleteItemHandler`": the early-reject behavior is the right call here.
4. **Map attachments** to `List<Pair<Integer, Long>>` shape that the legacy
   `MailManager` API expects.
5. **Hop to host**: `ctx.host().sync(() -> doSend(...))`. The whole MailManager
   call mutates in-memory state + DB + sends a packet to the online recipient,
   so the host-thread hop is mandatory.
6. **Inside the hop**:
    - Call `MailManager.sendSystemMailWithCustomAuthorBatching(author, charId, title, body, attachments)`.
    - Catch `IllegalArgumentException` → reply `VALIDATION_FAILED, mailManager=<msg>`.
    - Any other exception propagates; consumer-side adapter wraps it as
      `INTERNAL_ERROR` + reply.
7. **Compose payload**:
    - `createdMailIds` = `result.getLeft()` widened from `int` to `Long`.
    - `itemErrors` = each reason string from `result.getRight()` wrapped in
      an `ItemDeliveryError(null, null, reason)` — see "Mapping `itemErrors`"
      below for why typed identity is left nullable.
8. **Reply** `CommandResult.success(payload)`.

The handler does NOT cache `correlationId` for dedup. Redelivery on consumer
restart re-creates mails. Until R7 (idempotency cache) lands, operators MUST
treat consumer flapping during paid mail-delivery flows as a real-money risk.

### Mitigations available before R7 ships

- **Platform-web-side dedup.** The send-side commerce flow can attach an
  idempotency token at its outbox and short-circuit republish-with-same-token
  before the record reaches Kafka. This protects the paid-mail path without
  any host change. Recommended for the first paid-mail flow cut over.
- **Operational compensation runbook.** If a paid-mail consumer flaps mid-batch,
  ops MUST audit the resulting mails by `correlationId` ↔
  `Message.author / charId / sent_at` triplet and manually compensate
  (delete the duplicate mail row + roll back the duplicate item grants).
  The host has NO automatic compensation today — this is a known operational
  cost during the at-least-once Phase-2 window.

## Why `itemErrors` on a success envelope

MailManager's contract is "deliver as much as you can; tell me what failed
per-line". The mail row itself succeeds even if some attachments could not be
created — the recipient sees the mail, sees the partial attachments, the
delivery is real. Modeling the partial failure as a success envelope with a
non-empty `itemErrors` list preserves that semantic without dropping the
diagnostic context. The platform inspects `payload.itemErrors` to surface the
partial failure to the operator-facing UI.

The alternative (separate error code with the partial-failure context in
`errorDetails`) would force the platform to switch on success/error first,
then re-parse the diagnostics — two code paths for one operational outcome.

## Mapping `itemErrors` from MailManager strings

`MailManager` reports per-line failures as opaque human-readable strings of
the form
`"Failed to create mail attachment itemId: <X>, count: <Y> for charId: <Z>"`.
The failure list does NOT correlate positionally with the inbound items —
errors only appear at positions that failed, so `reasons[i]` does not refer
to `requested[i]`.

The handler emits one `ItemDeliveryError` per reason string with
`itemTemplateId` and `count` left `null` and the reason text forwarded
verbatim. Operator-facing UIs read the reason; the `null` typed identity
signals "host could not attribute the failure to a specific inbound line".

If MailManager grows structured per-line error reporting in the future, the
handler can populate the typed fields without an api wire change — both
fields are already nullable.

### Migration sharp edge

Legacy bohpts `MailService.sendMailAndReply` joined per-line failure reasons
with `"\n"` into a single `message: String` field on `SendMailResponseV1`.
The new wire shape ships an array of `ItemDeliveryError` entries — one per
reason. **Platform-web consumers MUST iterate `payload.itemErrors[]` and
read each `reason` separately**; reading `payload.itemErrors[0].reason`
will only surface the first failure when multiple lines fail. Migration
audit checklist: search platform-web code for any place that read the
legacy `message` field and split it on `"\n"` — those call sites need to
switch to iterating the new array.

## Compile-time + runtime contract

- `SendMailCommand implements NxCommand<SendMailPayload>` — the marker's type
  parameter binds the reply shape.
- `SendMailHandler implements CommandHandler<SendMailCommand, SendMailPayload>` —
  the handler's reply type is forced to match by the SAM's bound
  `C extends NxCommand<R>`.
- Registration `commands.on(SendMailCommand.class, new SendMailHandler(...))` —
  the `<R, C extends NxCommand<R>>` upper bound on `NxCommands.on` rejects any
  handler with a different `R` at compile time.

There is no runtime way for the platform and the handler to disagree about the
reply shape. Wire-shape changes require an api version bump.
