# Commands — SendMail

> Owner: @n1rmata

## Problem

Bohpts platform sends system mails to characters from two operator surfaces — the
web admin UI and the Telegram bot — using a hand-rolled RabbitMQ wire
(`SendMailRequestV1` consumed by both `tg-to-<server>` and `admin-to-<server>`
queues, both dispatching to `MailService.sendMailAndReply`). The legacy surface has
the per-spec problems documented in [`commands/spec.md`](../commands/spec.md):
free-form `message` field doubling as both error string and partial-failure report,
no structured error code, correlation id in body instead of headers, autoAck before
handler runs, no host-thread hop. The [`commands`](../commands/spec.md) infrastructure
slice shipped the runtime; this slice is the first concrete-DTO migration off
RabbitMQ onto the new commands rail.

Per [`commands/spec.md` line 406-408](../commands/spec.md): migration is per-command
cutover. Web side feature-flags individual commands to Kafka or RabbitMQ during
transition; bohpts-core runs both consumer surfaces in parallel; the legacy DTO
case is removed only when the platform stops emitting it. This slice does NOT
remove the `RabbitMqTgConsumer` / `RabbitMqWebAdminConsumer` SendMailRequestV1
arms — those retire in a follow-up cleanup once the web side is fully cut over.

Audience: bohpts-core operators wiring the new handler; platform-side mail-flow
authors composing `SendMailCommand` records onto the commands topic.

## Requirements

> Sibling features carry the wire + dispatch plumbing:
> - [`commands`](../commands/spec.md) — Kafka topic + consumer + dispatch table +
    > reply path + heartbeat slot. UNCHANGED by this slice.
> - [`adapter-modules`](../adapter-modules/spec.md) — Tier-1 ServiceLoader-based
    > `AdapterModule` discovery used by bohpts to wire its handlers via
    > `BohptsCommandsModule`.

**Must:**

- [done] R1. `nx-gs-adapter-api.kafka.commands.mail.SendMailCommand` MUST ship
  as `NxCommand<SendMailPayload>` with the following fields:
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

- [done] R3. `nx-gs-adapter-api.kafka.commands.mail.SendMailPayload` MUST ship as
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

- [done] R5. `bohpts-core` MUST ship `l2e.gameserver.l2nx.commands.handlers.SendMailHandler`
  implementing `CommandHandler<SendMailCommand, SendMailPayload>`:
    - Wire-path null guard on `charId` / `title` →
      `error(VALIDATION_FAILED, "field", "<name>")`.
    - Per-`MailItem` validation: null entry, null `itemTemplateId`, null/non-positive
      `count` → `error(VALIDATION_FAILED, "items[<idx>].<field>", ...)`.
    - `CharacterDAO.getByCharId` lookup; absent → `error(NOT_FOUND, "entity", "character")`.
    - `ctx.host().sync(...)` hop, then call
      `MailManager.sendSystemMailWithCustomAuthorBatching` (existing fork API).
    - `IllegalArgumentException` from MailManager (blank title, invalid receiver) →
      `error(VALIDATION_FAILED, "mailManager", "<message>")`.
    - Other exceptions propagate; adapter auto-wraps as `INTERNAL_ERROR`.
    - On success, build `SendMailPayload` from the returned mail-id list +
      per-line error reasons (mapped to `ItemDeliveryError` entries).

- [done] R6. `bohpts-core` MUST register the handler in `BohptsCommandsModule.onConnect`
  alongside the existing `DeleteItemHandler` registration.

**Should:**

- [todo] R7. Idempotency cache on `correlationId` — re-publishing a `SendMailCommand`
  with the same correlation id on a flapping consumer creates duplicate mails;
  for paid deliveries from the platform's commerce flows this is a real-money
  incident. The Phase-2 commands runtime punts dedup to handlers per the
  at-least-once contract; this slice does NOT add a cache. Track as a follow-up.

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

- [resolved: Reply payload typed as `SendMailPayload` (not `Void`). The legacy
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
  [`docs/features/commands/spec.md`](../commands/spec.md)
- Legacy reference (RabbitMQ command surface, core side):
  `E:/projects/bohpts/bohpts-core/core/src/main/java/l2e/gameserver/infrastructure/rabbitMq/`
- Legacy reference (RabbitMQ DTO, web side):
  `E:/bohpts/code/bohpts-rabbitmq/src/main/java/com/bohpts/messaging/dto/SendMailRequestV1.java`
- Companion document:
  [`docs/features/commands-send-mail/tech.md`](./tech.md) — wire layout + handler walkthrough
