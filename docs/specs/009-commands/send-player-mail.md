# Commands — SendPlayerMail

> Owner: @n1rmata

## Problem

`SendMailCommand` ([`send-mail.md`](send-mail.md)) can only send system mail: the sender is a display
string and attachments are created from item templates. A player who acts with their own character
needs the character-sender counterpart: mail with text, items taken from the sender's inventory, adena
and cash on delivery (COD), sent from a character that may be online or offline. The command must be
safe against a retry and against a command that sat in the Kafka backlog while the game-server was
down, because it moves items and adena.

Audience: platform mail-flow authors composing the command; host operators wiring the handler.

## Requirements

> Siblings: [`spec.md`](spec.md) (rail: `OwnerVerified` R28, `COMMAND_EXPIRED` R23, at-most-once R11),
> [`send-mail.md`](send-mail.md) (system-mail command), [`catalog.md`](catalog.md) (wire contract entry).

**Must:**

- [todo] R1. `nx-gs-adapter-api.kafka.commands.mail.SendPlayerMailCommand` MUST ship as
  `NxCommand<SendPlayerMailResult>` and `OwnerVerified`, routed by the simple name `SendPlayerMailCommand`
  (enum constant name for the platform-side audit: `SEND_PLAYER_MAIL`).
- [todo] R2. Payload fields:

  | Field             | Type                  | Required | Constraints / notes                                                                                                                                      |
  | ----------------- | --------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `senderCharId`    | `Long`                | yes      | Character the mail is sent from; items and adena are taken from it                                                                                       |
  | `recipientCharId` | `Long`                | yes      | Single recipient; MUST differ from `senderCharId`                                                                                                        |
  | `title`           | `String`              | yes      | Non-blank; at most 128 chars and at most 255 UTF-8 bytes; text allowlist (R10)                                                                           |
  | `body`            | `String?`             | no       | `null` = empty body; at most 512 chars and within the byte limit of the body column; `\n` allowed, text allowlist (R10)                                  |
  | `items`           | `List<PlayerMailItem>` | no      | Lines `PlayerMailItem{Long itemId, Long count}`; `itemId` is an inventory instance id of the sender, `count` positive; no duplicate `itemId`; `null`/empty = no item attachments. An adena stack MUST NOT be listed here, adena goes only through `adena` |
  | `adena`           | `Long?`               | no       | Adena placed in the mail; positive when present; counts as one attachment line for the host attachment limit and for the fee                              |
  | `codPrice`        | `Long?`               | no       | Cash on delivery, positive when present; requires at least one attachment (`COD_REQUIRES_ITEMS`); payment goes to the sender                             |
  | `ownerVerified`   | `boolean`             | no       | See R28 in [`spec.md`](spec.md); absent reads as `false`                                                                                                 |
  | `deadline`        | `Instant`             | yes      | Execution deadline, see R5; same type and name as in `BuyFromPrivateStoreCommand`                                                                        |

  Naming follows the existing wire classes (`senderCharId` / `recipientCharId`, boxed `Long` ids). The line type
  `PlayerMailItem` lives next to the command in `kafka.commands.mail`; `MailItem` cannot be reused because it
  carries `itemTemplateId`.

  The command is produced only for the owner of the sender character: the platform issues it for a player
  acting with their own character (`ownerVerified` per R28) and never for staff acting from a character. The
  host does not rely on that and validates the wire itself (R10).

  Limits that depend on host configuration (attachment count, maximum adena for `adena` and `codPrice`) are
  enforced by the host, not by the wire type.

- [todo] R3. `SendPlayerMailResult` MUST carry `mailId` (`long`, the created mail) and `feeAdena`
  (`long`, adena the host charged the sender for sending).
- [todo] R4. The command MUST be **all-or-nothing**: either the mail exists with every requested
  attachment and the fee charged, or nothing was taken from the sender and no mail exists. There are no
  partial mails and no `itemErrors` analogue.
- [todo] R5. `deadline` MUST be checked before anything is read, charged or moved. A passed deadline
  yields the `COMMAND_EXPIRED` status (its own `CommandStatus`, rail R23, not a reason under `INVALID_STATE`)
  with no mutation. Mixed rollout: a host receiving a command with a `null` `deadline` treats it as "no
  expiry"; the platform is deployed after the host and always sets it.
- [todo] R6. The command MUST be idempotent per `correlationId`. The platform reuses a stable
  correlation id for a retry of the same logical send; the host keeps durable per-command receipts
  keyed by that id (not the in-memory `CommandDedupStore`). A replay of a completed send returns the
  stored `mailId`; a replay while the first attempt is in flight reports an unknown outcome
  (`INTERNAL_ERROR` class with reason `OUTCOME_UNKNOWN`, not a business refusal) instead of executing again;
  the platform keeps its guard held and marks the reply `retrySafe = false`.
- [todo] R7. Every non-OK business reply MUST carry a stable machine-readable `reason` in
  `CommandProblem.extensions` (see Status mapping); the host never sends player-facing text.
- [todo] R8. `ownerVerified = true` MUST bypass the character item lock (a device-bound lock the
  command cannot satisfy), consistent with R28. This is deliberate: the verified master-account owner
  stands in for the device. `false` (staff acting on any character, or no fresh verification) leaves
  the lock in force.

- [todo] R10. The host MUST validate the wire command itself before the receipt claim (step 0), whatever
  the platform checked: required fields present; every id within the `int` range (out of range is
  `VALIDATION_FAILED` with a `field` extension); counts positive; no duplicate `itemId`; `codPrice` without
  any attachment is `VALIDATION_FAILED` with reason `COD_REQUIRES_ITEMS`; `title` at most 128 chars AND at
  most 255 UTF-8 bytes; `body` at most 512 chars and within its column byte limit; title and body pass the
  host chat-text allowlist (control characters and forged item-link tokens are refused). Too many
  attachments and too many body lines are both `VALIDATION_FAILED`.
- [todo] R11. Adena travels only through `adena`. An `items` line that points at an adena stack is
  `VALIDATION_FAILED`. The sender must hold `adena + fee` (one check); both are debited together and the
  adena is then added to the attachments.

**Should:**

- [todo] R9. Sender may be online or offline; the host serves both. Observable differences are limited
  to the `CHARACTER_BUSY` reason, which only an online sender can hit.

**Non-goals:**

- Fan-out: one command is one recipient. Multiple recipients are separate commands.
- System-sender mail and creating items from templates: that is `SendMailCommand`.
- Recipient-side accept / pay COD / refuse: done in the game client; COD expiry return is native.

### Edge cases

- **Sender online with the character busy** (trade, store, enchant, actions disabled, outside a peace
  zone with attachments): refused, `CHARACTER_BUSY`; only possible while online.
- **Sender goes online or offline while the command runs**: the host decides presence under a
  per-character mutation lock shared with login and logout, so a login waits for an offline send to
  commit and a logout waits for an online send; no half-transferred state is observable. If the lock
  cannot be taken within its short timeout the reply is `CHARACTER_BUSY`.
- **`itemId` that is not in the sender's inventory**: `ITEM_NOT_TRANSFERABLE` with the `itemId` extension.
- **Retry after `TIMEOUT` on the platform side**: safe, same `correlationId` replays the stored outcome.
- **Fee**: charged on top of attached adena; a sender who covers the attachment but not the fee gets
  `NOT_ENOUGH_ADENA`.

## Technical design

### Wire shape

Same rail as other commands: key `longBytesBe(senderCharId)` is the producer's choice, headers
`Nx-Message-Type = SendPlayerMailCommand`, `Nx-Correlation-Id` (stable per logical send),
`Nx-Target-Server-Id`. Reply type header is derived per rail R26: `SendPlayerMailResult`.

Example reply value (success):

```json
{ "status": "OK", "payload": { "mailId": 10243, "feeAdena": 1100 } }
```

Example reply value (refusal):

```json
{
  "status": "INVALID_STATE",
  "problem": {
    "title": "Cannot send mail",
    "extensions": { "reason": "RECIPIENT_INBOX_FULL" }
  }
}
```

### Status mapping

Same convention as the private-store buy reasons: the status classifies the refusal, the reason (in
`extensions.reason`) names it.

| Status              | `extensions.reason`                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------- |
| `NOT_FOUND`         | `RECIPIENT_NOT_FOUND`                                                                                       |
| `FORBIDDEN`         | `CHARACTER_BANNED`, `CHARACTER_JAILED`, `TRANSACTIONS_FORBIDDEN` (access level), `ITEM_LOCKED`, `RECIPIENT_BLOCKED_SENDER`, `RECIPIENT_GM_ONLY` |
| `VALIDATION_FAILED` | `SELF_SEND`, `TOO_MANY_ATTACHMENTS`, `TOO_MANY_LINES`, `ADENA_OVERFLOW`, `COD_REQUIRES_ITEMS`, `ITEM_NOT_TRANSFERABLE` (+ `itemId`); malformed command (null required field, id out of `int` range, bad count, duplicate `itemId`, adena stack in `items`, text outside the allowlist, title or body too long) carries a `field` extension instead |
| `INVALID_STATE`     | every other business refusal: `NOT_ENOUGH_ADENA`, `RECIPIENT_INBOX_FULL`, `OUTBOX_FULL`, `CHARACTER_BUSY`, `MAIL_DISABLED`, `ATTACHMENTS_DISABLED`, `LEVEL_TOO_LOW`, `RESTORE_IN_PROGRESS` |
| `COMMAND_EXPIRED`   | its own status (rail R23), no reason needed: the deadline passed, nothing changed                           |
| `INTERNAL_ERROR`    | `MAIL_DELIVERY_FAILED` (dirty rollback or persist failure, never a business refusal); `OUTCOME_UNKNOWN` for a replay that finds the first attempt still in flight (receipt `IN_FLIGHT`, `retrySafe = false`) |

`FEATURE_UNAVAILABLE` is platform-side only: the adapter replies `UNSUPPORTED_COMMAND` when no handler is
registered and the platform maps that to its 503.

### Host semantics (summary)

The handler is registered with plain `on(...)`, not `onDeduped`: dedup is the durable receipt (R6).

0. Wire validation (R10).
1. Deadline check (R5).
2. Claim the receipt for the `correlationId` with a pre-allocated mail id: already completed returns the
   stored `mailId`, in flight reports an unknown outcome, fresh continues.
3. Recipient re-check (exists, not self, GM-only recipients for GM senders, recipient block list,
   recipient inbox below its cap).
4. Sender gates: mail and attachments enabled, minimum level, access level, jail/ban, item lock (R8),
   outbox cap, attachment count, adena/COD caps. Ban and jail checks in the offline branch read the
   cached HWID/IP of the character plus account/character bans, because an offline load has no client.
5. Refuse with `RESTORE_IN_PROGRESS` while offline traders are still being restored (the sender included).
   Take the sender's per-character mutation lock (short timeout, `CHARACTER_BUSY` on timeout) and branch on
   presence; the presence check happens inside the lock, never before it.

- **Online sender**: native presence gates (`CHARACTER_BUSY`), per-item checks (in inventory, enough
  count, tradeable, not equipped, not manipulated by pet/mount/enchant, not quest), adena and fee
  covered, including instance-level transferability (time-limited, augmented where trading augmented
  items is disabled, skin activators) and `count == 1` for non-stackables. The sender identity (still the
  registered, online, not-logging-out instance) and the deadline are re-checked inside the host task before
  the first mutation. All lines are validated first; then the charge is recorded in the receipt before the
  debit, then adena and fee debit, item transfer into the mail, durable send. A failure after mutation
  started rolls the transferred items back and refunds the fee; a commit failure of the message insert is
  an unknown outcome (receipt held, no rollback), only a provably uncommitted transaction rolls back.
- **Offline sender**: one DB transaction under the same login lock held until commit: lock the sender's
  inventory rows, validate by template, move whole stacks or split partial ones, debit adena and fee,
  insert the mail row and the receipt together. Memory registration and recipient notification happen
  after commit.
- Fee is the native `100 + 1000 * attachments` adena (host config), so `feeAdena` is never `0` for an
  accepted mail. COD expiry return is native.
- Deliberate deviations from native mail: a jailed sender is always refused (native refuses only with
  attachments), and an inbox or outbox at the cap is refused (native tries to free a slot first).
- Epilogue: audit logs, `requestNow("character", ...)` for the sender and for the recipient, mail ingest
  through the existing mail sync.
- Boot recovery resolves a receipt left in flight: mail row present means OK and the stored result is
  materialized (a replay returns the `mailId`, never `null`); absent means FAILED. Orphaned items are
  returned only by the object ids recorded in the receipt claim, never by a location scan. A receipt that
  recorded the charge but no outcome is held for an operator alert, with no automatic refund.

Exact storage layout and transaction details are the host's documentation, not this contract.

### Platform side

Platform order of checks, the `requestId` to `correlationId` mapping, ownership and ban-zone rules and
the REST contract live in `nx-gameservers/docs/specs/037-mail/player-send.md`; the command is issued
with `CommandInitiator.user(...)` and a `deadline` of now plus the platform reply timeout.

## Rollout

Additive wire (new command, new DTOs, new optional field on `SendMailCommand`, see
[`send-mail.md`](send-mail.md)); the adapter api version is bumped. Order: release `api/vX.Y.Z` first, then
the host, then the platform (the platform is deployed after the host and always sets `deadline`; the host treats
a `null` `deadline` as "no expiry", R5). Before the host registers the handler it answers
`UNSUPPORTED_COMMAND`, which the platform maps to `FEATURE_UNAVAILABLE` (503); no compatibility straddle.

`SendPlayerMailCommand` joins the `OwnerVerified` list (`spec.md` R28, `catalog.md` › Owner verification).

## Open questions

None. Resolved: `feeAdena` is the amount for this mail only and is never `0`; the sync side effect is
`requestNow` for the entity `character` with the sender and the recipient.

## Links

- Counterpart (system-sender command): [`send-mail.md`](send-mail.md)
- Rail: [`spec.md`](spec.md); wire entry: [`catalog.md`](catalog.md)
- Platform contract and REST flow: `nx-gameservers/docs/specs/037-mail/player-send.md`
- Delivery framework (deadline, durable receipts): `nx-gameservers/docs/specs/068-critical-commands-framework.md`
- Owner verification on the platform: `nx-gameservers/docs/specs/053-character-ownership.md`
- Host documentation: not linked here, this repository does not name tenant hosts.
