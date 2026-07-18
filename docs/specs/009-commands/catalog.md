# Commands — Wire Catalog

> Per-command business contract. For the runtime / handler-author concerns
> (lifecycle, threading, registration, error envelope) see
> [`guide.md`](./guide.md). Formal infra spec lives in [`spec.md`](./spec.md).

This document describes **what each command does** from the platform's
point of view — the inputs the caller supplies, the result fields they
receive on success, and the error statuses they can expect. It is the
contract every web-side caller of `NxCommandSender.sendSync(...)` is
coding against.

All commands ship under `app.l2nx.gs.adapter.api.kafka.commands.<group>.*`
where group is one of: `item`, `character`, `mail`, `telegram`. The
group is purely a code-organization split; on the wire every command
travels on the single `<tenant>.gs.commands` topic, routed by the
`Nx-Message-Type` header.

Naming convention: `{X}Command` → `{X}Result`. The reply envelope is
always `CommandResult<R>` where `R` is the dedicated result type
declared on the command's `NxCommand<R>` marker.

---

## Item commands

### `CreateItemCommand`

**Purpose.** Grant a fresh item stack to a character — admin item-grant,
event reward, support compensation.

**Inputs**

| Field            | Type            | Required | Notes                                                                             |
|------------------|-----------------|----------|-----------------------------------------------------------------------------------|
| `charId`         | `Long`          | yes      | Target character primary key                                                      |
| `itemTemplateId` | `Long`          | yes      | Catalog template id (e.g. `57` = adena). NOT a stack object-id                    |
| `count`          | `Long`          | yes      | Quantity to grant. Must be positive                                               |
| `enchantLevel`   | `Long?`         | no       | Default `0`. Applied to non-stackable templates only; ignored for stackable       |
| `location`       | `ItemLocation?` | no       | Default `INVENTORY`. Today supported: `INVENTORY`, `WH`. Others → `INVALID_STATE` |

**Result** (`CreateItemResult`)

| Field          | Type           | Notes                                                                                  |
|----------------|----------------|----------------------------------------------------------------------------------------|
| `itemId`       | `Long`         | Object-id of the resulting stack. For stackable + existing stack → existing stack's id |
| `countCreated` | `Long`         | Echo of `count` (host may clamp on stack-size limits — typically equal to input)       |
| `enchantLevel` | `Long`         | Actually applied enchant (0 for stackable templates regardless of request)             |
| `location`     | `ItemLocation` | Echo of `location` for caller confirmation                                             |

**Errors**

| Status              | When                                                                   |
|---------------------|------------------------------------------------------------------------|
| `NOT_FOUND`         | Character does not exist (charId not in DB), or template id unknown    |
| `INVALID_STATE`     | Inventory full / weight cap exceeded, or unsupported `location` for v1 |
| `VALIDATION_FAILED` | Missing required field, non-positive `count`, out-of-int-range id      |
| `INTERNAL_ERROR`    | Unexpected failure (IdFactory exhausted, DB write threw, etc.)         |

**Side effects on success.** Host emits `NxSync.requestNow("item", itemId)` and
`requestNow("character", charId)` — sync engine ticks both entities on the
next cycle so platform consumers observe the new stack within ms instead
of waiting up to one CDC interval.

---

### `DeleteItemCommand`

**Purpose.** Remove items from a character — confiscation, refund, cleanup
after rollback. Partial-stack semantics: `count` is clamped to the live
stack size, never errors on excess.

**Inputs**

| Field    | Type   | Required | Notes                                                          |
|----------|--------|----------|----------------------------------------------------------------|
| `charId` | `Long` | yes      | Owner character primary key                                    |
| `itemId` | `Long` | yes      | Object-id of the specific stack (NOT template id)              |
| `count`  | `Long` | yes      | Quantity to remove. Must be positive. Clamped to current stack |

**Result** (`DeleteItemResult`)

| Field          | Type      | Notes                                                                                          |
|----------------|-----------|------------------------------------------------------------------------------------------------|
| `itemId`       | `Long`    | Echo of the stack that was decremented / destroyed                                             |
| `countDeleted` | `Long`    | Actual count removed — MAY be less than requested when the live stack was smaller              |
| `fullyDeleted` | `boolean` | `true` if the stack is gone (object destroyed); `false` if it was decremented and still exists |

**Errors**

| Status              | When                                                                       |
|---------------------|----------------------------------------------------------------------------|
| `NOT_FOUND`         | No stack with that object-id owned by this character                       |
| `INVALID_STATE`     | Item in an unsupported location (auction, freight, etc.); destroy rejected |
| `VALIDATION_FAILED` | Missing required field, non-positive `count`                               |
| `UNAVAILABLE`       | DB error on offline path                                                   |
| `INTERNAL_ERROR`    | Unexpected handler failure                                                 |

**Side effects on success.** `requestNow("item", itemId)` and
`requestNow("character", charId)`.

---

### `TransferItemToCharacterCommand`

**Purpose.** Move a stack from one character to another. Transparently
handles all four online/offline source-target combinations.

**Inputs**

| Field        | Type   | Required | Notes                                            |
|--------------|--------|----------|--------------------------------------------------|
| `charIdFrom` | `Long` | yes      | Source character                                 |
| `charIdTo`   | `Long` | yes      | Target character (must differ from `charIdFrom`) |
| `itemId`     | `Long` | yes      | Object-id of the source stack                    |
| `count`      | `Long` | yes      | Quantity to move. Clamped to source stack size   |

**Result** (`TransferItemToCharacterResult`)

| Field              | Type   | Notes                                           |
|--------------------|--------|-------------------------------------------------|
| `itemId`           | `Long` | Echo of the moved stack's id                    |
| `countTransferred` | `Long` | Actual count moved (may be less than requested) |
| `fromCharId`       | `Long` | Echo                                            |
| `toCharId`         | `Long` | Echo                                            |

**Errors**

| Status              | When                                                                             |
|---------------------|----------------------------------------------------------------------------------|
| `NOT_FOUND`         | Either character or the item not found                                           |
| `INVALID_STATE`     | Target inventory capacity / weight cap exceeded; item in an unsupported location |
| `VALIDATION_FAILED` | Same-character transfer, missing field, non-positive count                       |
| `INTERNAL_ERROR`    | Unexpected handler failure                                                       |

**Side effects on success.** `requestNow("item", itemId)` and
`requestNow("character", [fromId, toId])`.

---

## Character commands

### `TransferCharToAccountCommand`

**Purpose.** Re-bind a character to a different account (account migration,
re-sale support). Forces logout if the source player is online so the
target account picks the character up cleanly on next login.

**Inputs**

| Field       | Type     | Required | Notes                          |
|-------------|----------|----------|--------------------------------|
| `charId`    | `Long`   | yes      | Character to re-bind           |
| `accountTo` | `String` | yes      | Destination login (must exist) |

**Result** (`TransferCharToAccountResult`)

| Field            | Type      | Notes                                          |
|------------------|-----------|------------------------------------------------|
| `charId`         | `Long`    | Echo                                           |
| `newAccountName` | `String`  | Echo of destination account                    |
| `wasLoggedOut`   | `boolean` | `true` if the player was online and got kicked |

**Errors**

| Status              | When                                                |
|---------------------|-----------------------------------------------------|
| `NOT_FOUND`         | Character or destination account does not exist     |
| `INVALID_STATE`     | Character cannot be re-bound (clan-leader, jail, …) |
| `VALIDATION_FAILED` | Missing fields                                      |
| `INTERNAL_ERROR`    | Unexpected failure (DB write, etc.)                 |

**Side effects on success.** `requestNow("character", charId)`.

---

## Mail commands

### `SendMailCommand`

**Purpose.** Compose and deliver an in-game mail to a character with
optional item attachments. Partial-success aware: some attachments may
fail to materialize (unknown template, capacity exceeded for non-stackable),
the mail itself is still delivered with whatever succeeded.

**Inputs**

| Field    | Type             | Required | Notes                                                  |
|----------|------------------|----------|--------------------------------------------------------|
| `charId` | `Long`           | yes      | Recipient character                                    |
| `author` | `String?`        | no       | Display author. Defaults to system character on host   |
| `title`  | `String`         | yes      | Mail subject (≤ 128 chars per L2 wire limit)           |
| `body`   | `String?`        | no       | Mail body (≤ 512 chars per L2 wire limit)              |
| `items`  | `List<MailItem>` | no       | Each `MailItem`: `itemTemplateId: Long`, `count: Long` |

**Result** (`SendMailResult`)

| Field            | Type                      | Notes                                                               |
|------------------|---------------------------|---------------------------------------------------------------------|
| `createdMailIds` | `List<Long>`              | Host mail-row ids that landed (1 per mail row; usually one entry)   |
| `itemErrors`     | `List<ItemDeliveryError>` | Per-item failures (template id + reason). Empty list = full success |

**Errors**

| Status              | When                                        |
|---------------------|---------------------------------------------|
| `NOT_FOUND`         | Recipient character does not exist          |
| `VALIDATION_FAILED` | Missing required field, title too long, ... |
| `INTERNAL_ERROR`    | Mail subsystem rejected the write           |

**Side effects on success.** `requestNow("character", charId)`. Mail
entity is its own sync subject when configured.

---

## Telegram commands

### `TelegramCharLinkCommand`

**Purpose.** Issue a Telegram-to-character link verification code. Sends
the code as an in-game system message to the resolved character; user
enters it back into the Telegram bot to confirm ownership.

**Inputs**

| Field              | Type     | Required | Notes                                                      |
|--------------------|----------|----------|------------------------------------------------------------|
| `accountName`      | `String` | yes      | Account login owning the character                         |
| `charName`         | `String` | yes      | Character name (case-insensitive match within the account) |
| `confirmationCode` | `String` | yes      | Caller-generated short code (e.g. 6 alphanumerics)         |
| `telegramUserId`   | `Long`   | yes      | Telegram user-id for audit and uniqueness                  |

**Result** (`TelegramCharLinkResult`)

| Field    | Type   | Notes                                                                      |
|----------|--------|----------------------------------------------------------------------------|
| `charId` | `Long` | Resolved character id (caller stores this against its pending-link record) |

**Errors**

| Status              | When                                                                    |
|---------------------|-------------------------------------------------------------------------|
| `NOT_FOUND`         | No character with that name on that account                             |
| `VALIDATION_FAILED` | Missing fields                                                          |
| `INTERNAL_ERROR`    | Could not deliver the in-game system message (rare; logged on the host) |

**Side effects on success.** None — link verification is a read-only
resolve + side-channel send; no sync trigger needed.

---

## Cross-cutting

### Sync triggers

Every command that mutates persistent state calls
`ctx.sync().requestNow(entity, pk)` on the success path. Platform consumers
observe the change on the next sync cycle (typically within 1–2 s of the
reply) instead of waiting up to the configured CDC interval (default
60 s).

Mapping:

| Command                          | Triggered entities                                |
|----------------------------------|---------------------------------------------------|
| `CreateItemCommand`              | `item` (created stack), `character` (owner)       |
| `DeleteItemCommand`              | `item` (decremented/destroyed stack), `character` |
| `TransferItemToCharacterCommand` | `item` (moved stack), `character` (from + to)     |
| `TransferCharToAccountCommand`   | `character` (re-bound)                            |
| `SendMailCommand`                | `character` (recipient)                           |
| `TelegramCharLinkCommand`        | none                                              |

### Status semantics

`CommandStatus` carries a `Tier` for coarse caller routing:

- **`OK`** → apply payload
- **`CLIENT_ERROR`** (`NOT_FOUND` / `INVALID_STATE` / `FORBIDDEN` /
  `VALIDATION_FAILED` / `RATE_LIMITED` / `UNSUPPORTED_COMMAND`) → surface
  to user; do not retry
- **`SERVER_ERROR`** (`UNAVAILABLE` / `INTERNAL_ERROR`) → alert ops; retry
  may succeed

`UNSUPPORTED_COMMAND` is adapter-emitted only — it means the deployed
core has no handler registered for the command type. Indicates a deploy
skew between platform and core.

### Identifier conventions

- `itemTemplateId` — catalog template (e.g. `57` = adena). Stable across
  servers and across the item's lifetime.
- `itemId` — per-stack object-id assigned by the host on creation.
  Unique per game-server lifetime; identifies one specific stack.
- `charId` — character primary key in the host DB. Stable.
- All wire ids are `Long` to accommodate hosts that may use 64-bit ids;
  hosts using `int` internally bounds-check + downcast at the handler
  boundary.

### Partial success

`SendMailCommand` is the only command with first-class partial-success
semantics: `SendMailResult.itemErrors` enumerates per-item failures while
the mail itself was delivered. All other commands are all-or-nothing —
either `OK` with the full effect applied, or a non-OK status with no
state change.
