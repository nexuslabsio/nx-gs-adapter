# Commands — SendMail

> Owner: @n1rmata

## Problem

Bohpts platform sends system mails to characters from two operator surfaces — the
web admin UI and the Telegram bot — using a hand-rolled RabbitMQ wire
(`SendMailRequestV1` consumed by both `tg-to-<server>` and `admin-to-<server>`
queues, both dispatching to `MailService.sendMailAndReply`). The legacy surface has
the per-spec problems documented in [`commands/spec.md`](009-commands/spec.md):
free-form `message` field doubling as both error string and partial-failure report,
no structured error code, correlation id in body instead of headers, autoAck before
handler runs, no host-thread hop. The [`commands`](009-commands/spec.md) infrastructure
slice shipped the runtime; this slice is the first concrete-DTO migration off
RabbitMQ onto the new commands rail.

Per [`commands/spec.md` line 406-408](009-commands/spec.md): migration is per-command
cutover. Web side feature-flags individual commands to Kafka or RabbitMQ during
transition; bohpts-core runs both consumer surfaces in parallel; the legacy DTO
case is removed only when the platform stops emitting it. This slice does NOT
remove the `RabbitMqTgConsumer` / `RabbitMqWebAdminConsumer` SendMailRequestV1
arms — those retire in a follow-up cleanup once the web side is fully cut over.

Audience: bohpts-core operators wiring the new handler; platform-side mail-flow
authors composing `SendMailCommand` records onto the commands topic.

## Requirements

> Sibling features carry the wire + dispatch plumbing:
>
> - [`commands`](009-commands/spec.md) — Kafka topic + consumer + dispatch table + > reply path + heartbeat slot. UNCHANGED by this slice.
> - [`adapter-modules`](002-adapter-modules/spec.md) — Tier-1 ServiceLoader-based > `AdapterModule` discovery used by bohpts to wire its handlers via > `BohptsCommandsModule`.

**Must:**

- [done] R1. `nx-gs-adapter-api.kafka.commands.mail.SendMailCommand` MUST ship
  as `NxCommand<SendMailResult>` with the following fields:
  - `Long charId` — REQUIRED. Recipient character primary key.
  - `@Nullable String author` — OPTIONAL. Display name of the sender; null/blank
    delegates to the host's system-default sender name.
  - `String title` — REQUIRED. Subject line.
  - `@Nullable String body` — OPTIONAL. Body text; null treated as empty.
  - `@Nullable List<MailItem> items` — OPTIONAL. Attachment lines; null/empty
    produces a text-only mail. Stored as an unmodifiable list; null normalized
    to `Collections.emptyList()` on read.

  Constructor enforces non-null `charId` and `title` via `IllegalArgumentException`
  for programmatic construction. Wire-path Gson bypasses the constructor; handler
  re-validates and emits `VALIDATION_FAILED` on missing fields.

- [done] R2. `nx-gs-adapter-api.kafka.commands.mail.MailItem` MUST ship as a POJO
  with two fields: `Long itemTemplateId` (REQUIRED) and `Long count` (REQUIRED,
  positive). Constructor rejects null on either + non-positive `count` via IAE.
  Builder defaults `count` to `1`. Handler re-validates the wire path.

  Naming: `itemTemplateId` (not `itemId`) is deliberate — it is the catalog
  template id from which the attachment stack is created, NOT the per-instance
  object-id used by `DeleteItemCommand`. Different identity, different lifetime.

- [done] R3. `nx-gs-adapter-api.kafka.commands.mail.SendMailResult` MUST ship as
  the success-payload type carrying:
  - `List<Long> createdMailIds` — primary keys of the materialized mail rows.
    Multi-element when the host batches the inbound `items` across multiple
    mails (host-defined cap; bohpts uses `Config.MAIL_MAX_ATTACHMENTS`).
    Non-null on read; null in constructor normalized to empty list.
  - `List<ItemDeliveryError> itemErrors` — partial-failure entries when
    specific attachment lines could not be materialized. Non-null on read;
    empty list signals "all attachments materialized successfully". Non-empty
    `itemErrors` on a `success` envelope means partial delivery — the mail
    was sent but without the listed attachments.

  Both lists are defensively copied + frozen on construction (unmodifiable view
  from getters).

- [done] R4. `nx-gs-adapter-api.kafka.commands.mail.ItemDeliveryError` MUST ship
  as a POJO with three fields: `@Nullable Long itemTemplateId`,
  `@Nullable Long count`, `String reason`. Identity fields are best-effort —
  populated only when the host can confidently attribute the failure to a
  specific inbound line. `reason` is the always-present diagnostic; null
  passed to the constructor is normalized to an empty string at construction
  so getters and `equals`/`hashCode`/`toString` agree.

- [done] R5. `bohpts-core` MUST ship `l2e.gameserver.l2nx.commands.mail.SendMailHandler`
  implementing `CommandHandler<SendMailCommand, SendMailResult>`:
  - Wire-path null guard on `charId` / `title` →
    `validationFailed("<title>", "<name>")` (the convenience factory puts the offending
    field under the `field` extension key).
  - Per-`MailItem` validation: null entry, null `itemTemplateId`, null/non-positive
    `count` → `validationFailed("<title>", "items[<idx>].<field>")`.
  - `CharacterDAO.getByCharId` lookup; absent →
    `notFound("<title>", "entity", "character")`.
  - `ctx.host().sync(...)` hop, then delegate to `l2nx.commands.mail.MailService`, which
    calls `MailManager.sendSystemMailWithCustomAuthorBatching` (existing fork API).
  - `IllegalArgumentException` from MailManager (blank title, invalid receiver) →
    `error(VALIDATION_FAILED, "<title>", "mailManager", "<message>")`.
  - Other exceptions propagate; adapter auto-wraps as `INTERNAL_ERROR`.
  - On success, build `SendMailResult` from the returned mail-id list +
    per-line error reasons (mapped to `ItemDeliveryError` entries).

- [done] R6. `bohpts-core` MUST register the handler in `BohptsCommandsModule.onConnect`
  alongside the existing `DeleteItemHandler` registration.

**Should:**

- [partially done] R7. Idempotency cache on `correlationId`. The handler IS registered
  through `BohptsCommandsModule.onDeduped`, so `DedupCommandHandler` + `CommandDedupStore`
  (in-memory, OK-only, 10 min TTL, lost on restart) sit in front of it. That barrier is
  currently inert for the admin surface: the platform mints a fresh correlation id per
  dispatch, so a retry never matches a cached entry. Making it effective requires the
  platform to persist and reuse the id, plus a durable receipt to survive a game-server
  restart — see `nx-gameservers/docs/specs/068-critical-commands-framework.md` (L2, L5).

**Could:**

- [todo] R8. Migration to a `SendMailWithReplyToCommand` variant that carries
  thread-id / reply-to mail id metadata for non-system mail. Not in scope —
  bohpts platform only sends system mails through this surface today.

**Non-goals:**

- **Removing legacy RabbitMQ `SendMailRequestV1` cases.** Per per-command cutover
  the legacy switch arms in `RabbitMqTgConsumer` / `RabbitMqWebAdminConsumer`
  remain live until the web side stops emitting on RabbitMQ.
- **Server-side validation of mail-inbox capacity.** The legacy
  `MailManager.sendSystemMailWithCustomAuthorBatching` does not gate on inbox
  capacity (it just inserts), and that semantic carries over here.
- **`reloadInMemoryMail` migration.** That admin command is a different DTO and
  ships in a separate slice.

### Edge cases

- **`charId` not found and platform has stale character state.** Reply
  `NOT_FOUND` → web side decides whether to alert / retry / drop. Same as legacy.
- **Empty `items` + empty `body` + non-blank `title`** — text-only mail with
  no attachments and a header-only body. Allowed; MailManager creates a single
  mail row with no attachments.
- **Single `items` entry with a stack so large MailManager rejects the
  attachment** — partial-success: `createdMailIds` has one entry (the mail
  itself was sent), `itemErrors` has one entry describing the rejection.
  Reply envelope is `success` (legacy semantic preserved).
- **`items.size() > Config.MAIL_MAX_ATTACHMENTS`** — MailManager batches into
  multiple mails titled `"<title> 1/N"` … `"<title> N/N"`; `createdMailIds`
  carries one id per resulting mail. `itemErrors` aggregates per-line failures
  across all batches.
- **Re-delivery of the same correlation id mid-mail-creation** — duplicate
  mails created. R7 tracks the cache; until then, ops MUST be aware.

## Open questions

- [resolved: Reply payload typed as `SendMailResult` (not `Void`). The legacy
  `Set<Integer> createdMailIds` is load-bearing — web side needs the ids to
  link back to the originating delivery, especially for audit / refund flows.]
- [resolved: Partial item-failures on a `success` envelope (not a separate
  error code). The mail itself was delivered; per-attachment failures are
  diagnostic, not the operation's outcome. Matches legacy
  `CommonStatus.SUCCESS + message=<errors>` semantic.]
- [resolved: `itemTemplateId` rename vs legacy `itemId`. The legacy field was
  ambiguous (could be read as object-id) and the new commands surface uses
  `itemObjectId` for object-ids in `DeleteItemCommand`. Renaming makes the
  identity explicit.]
- [assumed: `Long createdMailIds` (not `Integer`). Aligns with the rest of
  the commands api which uses `Long` for primary keys; current bohpts
  `Message::getId` returns `int` so the host widens on the way out.]

## Links

- Sibling feature (commands runtime + dispatch + reply path):
  [`docs/specs/009-commands/spec.md`](009-commands/spec.md)
- Platform counterpart (send path, ingest, defects, planned outbox/idempotency layers):
  `nx-gameservers/docs/specs/037-mail/sync.md`
- Delivery/idempotency framework this command is expected to adopt (deadline gate, durable receipts,
  safe-set outcome rules): `nx-gameservers/docs/specs/068-critical-commands-framework.md`
- Legacy reference (RabbitMQ command surface, core side):
  `bohpts-core/core/src/main/java/l2e/gameserver/infrastructure/rabbitMq/`
- Legacy reference (RabbitMQ DTO, web side):
  `bohpts-rabbitmq/src/main/java/com/bohpts/messaging/dto/SendMailRequestV1.java`
- Companion document: see the Technical design section below — wire layout + handler walkthrough

---

## Technical design

### Wire shape

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
value: gson(CommandResult<SendMailResult> JSON)
```

Example value (success with batched mails + one partial failure):

```json
{
  "status": "OK",
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
  "status": "NOT_FOUND",
  "problem": {
    "title": "Character not found",
    "extensions": { "entity": "character" }
  }
}
```

### Field reference

| Field                    | Type          | Required | Notes                                            |
| ------------------------ | ------------- | -------- | ------------------------------------------------ |
| `charId`                 | `Long`        | Yes      | Recipient character pk                           |
| `author`                 | `String?`     | No       | Sender display; null/blank → host system default |
| `title`                  | `String`      | Yes      | Mail subject; MailManager rejects blank          |
| `body`                   | `String?`     | No       | Mail body; null → empty                          |
| `items`                  | `[MailItem]?` | No       | Attachments; null/empty → text-only mail         |
| `items[].itemTemplateId` | `Long`        | Yes      | Catalog template id (NOT object-id)              |
| `items[].count`          | `Long`        | Yes      | Stack size, positive                             |

Reply payload:

| Field                         | Type                  | Notes                                                        |
| ----------------------------- | --------------------- | ------------------------------------------------------------ |
| `createdMailIds`              | `[Long]`              | One entry per resulting mail (host-side batching)            |
| `itemErrors`                  | `[ItemDeliveryError]` | Per-line failures; non-empty + success = partial delivery    |
| `itemErrors[].itemTemplateId` | `Long?`               | Optional inbound-line hint; `null` when host can't attribute |
| `itemErrors[].count`          | `Long?`               | Optional inbound-line hint; `null` when host can't attribute |
| `itemErrors[].reason`         | `String`              | Free-form diagnostic; null normalized to ""                  |

### Status mapping

| Condition                                       | Status              | `problem.extensions`                                       |
| ----------------------------------------------- | ------------------- | ---------------------------------------------------------- |
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
| Per-attachment-line failure                     | (`OK` envelope)     | `payload.itemErrors[].reason`                              |

### Handler walkthrough

`l2e.gameserver.l2nx.commands.mail.SendMailHandler.handle(cmd, ctx)`:

1. **Wire-validate** `charId` / `title` non-null (Gson bypasses ctor).
2. **Wire-validate** every `MailItem`: non-null entry, non-null `itemTemplateId`,
   non-null + positive `count`. Index-tagged details so the platform can pinpoint
   the bad line.
3. **DAO lookup** `CharacterDAO.getByCharId(cmd.getCharId().intValue())` — runs
   on `ctx.io()` (the adapter-owned IO pool), not on the consumer thread and
   not via `ctx.host().sync(...)`. Blocking JDBC belongs on the IO pool —
   running it on the consumer thread blocks the topic, and running it through
   the game executor burns capacity meant for game-state mutations. The
   `DeleteItemHandler` pattern is the canonical model for handler IO work
   (long → int wire-id bound check, `ctx.io()` for the JDBC, transactional
   path with `SELECT ... FOR UPDATE` + login re-check where appropriate,
   capture mutation return values, reply `INVALID_STATE` on 0-rows). There is
   a TOCTOU window — the recipient could be deleted between the lookup and
   the host-thread hop — but the resulting orphan mail row is recoverable
   and the legacy RabbitMQ path (`MailService.sendMailAndReply`) has the
   same race.
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

The handler IS wrapped for correlation-id dedup: `BohptsCommandsModule` registers it
through `onDeduped`, so `DedupCommandHandler` + `CommandDedupStore` (in-memory,
OK-only, 10 min TTL, lost on restart) sit in front of it. The commands consumer
is at-most-once (offsets committed before dispatch), so Kafka redelivery is not
the duplicate source — a platform re-issue is.

That barrier is currently inert for the admin surface: the platform mints a fresh
correlation id per dispatch, so a retry never matches a cached entry. Making it
effective needs the platform to persist and reuse the id, plus a durable receipt
to survive a game-server restart — see
`nx-gameservers/docs/specs/068-critical-commands-framework.md` (L2, L5), and note
its warning that a shared receipts table without a per-command STARTED resolver
turns boot recovery into a duplicate generator for mail.

#### Mitigations until the platform reuses the correlation id

- **Platform-side idempotency key.** The send-side flow attaches a stable token and
  reuses it on retry, so the existing dedup decorator actually fires.
- **Operational compensation runbook.** If a paid-mail flow is retried mid-batch,
  ops MUST audit the resulting mails by `correlationId` <->
  `Message.author / charId / sent_at` triplet and manually compensate
  (delete the duplicate mail row + roll back the duplicate item grants).
  The host has NO automatic compensation today.

### Why `itemErrors` on a success envelope

MailManager's contract is "deliver as much as you can; tell me what failed
per-line". The mail row itself succeeds even if some attachments could not be
created — the recipient sees the mail, sees the partial attachments, the
delivery is real. Modeling the partial failure as a success envelope with a
non-empty `itemErrors` list preserves that semantic without dropping the
diagnostic context. The platform inspects `payload.itemErrors` to surface the
partial failure to the operator-facing UI.

The alternative (a separate non-OK `CommandStatus` with the partial-failure
context in `CommandProblem.extensions`) would force the platform to switch
on OK/non-OK first, then re-parse the diagnostics — two code paths for one
operational outcome.

### Mapping `itemErrors` from MailManager strings

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

#### Migration sharp edge

Legacy bohpts `MailService.sendMailAndReply` joined per-line failure reasons
with `"\n"` into a single `message: String` field on `SendMailResponseV1`.
The new wire shape ships an array of `ItemDeliveryError` entries — one per
reason. **Platform-web consumers MUST iterate `payload.itemErrors[]` and
read each `reason` separately**; reading `payload.itemErrors[0].reason`
will only surface the first failure when multiple lines fail. Migration
audit checklist: search platform-web code for any place that read the
legacy `message` field and split it on `"\n"` — those call sites need to
switch to iterating the new array.

### Compile-time + runtime contract

- `SendMailCommand implements NxCommand<SendMailResult>` — the marker's type
  parameter binds the reply shape.
- `SendMailHandler implements CommandHandler<SendMailCommand, SendMailResult>` —
  the handler's reply type is forced to match by the SAM's bound
  `C extends NxCommand<R>`.
- Registration `commands.on(SendMailCommand.class, new SendMailHandler(...))` —
  the `<R, C extends NxCommand<R>>` upper bound on `NxCommands.on` rejects any
  handler with a different `R` at compile time.

There is no runtime way for the platform and the handler to disagree about the
reply shape. Wire-shape changes require an api version bump.
