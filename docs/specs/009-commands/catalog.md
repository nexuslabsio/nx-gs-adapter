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
where group is one of: `announcement`, `ban`, `character`, `gd`, `item`,
`mail`, `privatestore`, `sync`, `telegram`. The group is purely a
code-organization split; on the wire every command travels on the single
`<tenant>.gs.commands` topic, routed by the `Nx-Message-Type` header.

Naming convention: `{X}Command` → `{X}Result`. The reply envelope is
always `CommandResult<R>` where `R` is the dedicated result type
declared on the command's `NxCommand<R>` marker.

---

## Announcement commands

### `AnnounceNowCommand`

**Purpose.** Broadcast a one-shot chat announcement to the game-server —
the platform's scheduler (or an operator's "send now" action) decides
_when_ to fire; this command carries only the final text and channel,
nothing about scheduling or origin. NOT idempotent — re-delivery (e.g.
Kafka redelivery on crash recovery) re-broadcasts the message; announcements
carry no unique id to dedupe on.

**Inputs**

| Field      | Type      | Required | Notes                                                                                                                                                                                                          |
| ---------- | --------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `text`     | `String`  | yes      | Neutral chat micro-format: plain text, literal `\n` hard line breaks, bare `http(s)://` URLs for auto-linking. Never carries the bohpts-specific `/n` or `[=url=]` wire tokens — translating is a host concern |
| `critical` | `boolean` | yes      | `false` = normal announcement channel, `true` = the more visible critical/alert channel. Applies to the whole message                                                                                          |

**Result** (`AnnounceResult`)

| Field       | Type  | Notes                                                                                                                                                                                       |
| ----------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `linesSent` | `int` | Number of physical chat lines actually broadcast — typically the count of non-empty lines after splitting `text` on `\n`. Best-effort telemetry; hosts that don't track this MAY report `0` |

**Errors**

| Status              | When                                 |
| ------------------- | ------------------------------------ |
| `VALIDATION_FAILED` | Wire payload missing `text`          |
| `INTERNAL_ERROR`    | Broadcast mechanism failed host-side |

---

### `DeleteAutoAnnouncementCommand`

**Purpose.** Delete one row from the game-server's native
`auto_announcements` table (or equivalent). Used for two platform flows:
an operator deleting a `GAME`-origin row directly ("delete in game"), and
the `GAME`→`L2NX` transfer flow, where the platform first creates its own
copy of the announcement and then issues this command to remove the
now-redundant source row so it is not re-ingested by the next db-sync
cycle.

**Inputs**

| Field    | Type   | Required | Notes                                                                                                                              |
| -------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `gameId` | `long` | yes      | The host's native `auto_announcements` row id — the same value surfaced as `AutoAnnouncementDbDto.id` on the db-sync mirror stream |

**Result** — none (`Void`). Reply is `CommandResult<Void>` — `ok()` on
success with no typed payload; the caller already knows the `gameId` it
asked to delete.

**Errors**

| Status           | When                                  |
| ---------------- | ------------------------------------- |
| `NOT_FOUND`      | No row with the given `gameId` exists |
| `INTERNAL_ERROR` | Delete failed host-side               |

---

## Ban commands

### `BanCommand`

**Purpose.** Apply a ban. Build-agnostic: the contract names the target
dimension, the ban kind, and the expiry; the host maps those onto its own
ban engine and decides how the ban is enforced. Applying a ban is
naturally convergent — the host SHOULD treat a re-issued command for an
already-active ban as a no-op success.

**Inputs**

| Field         | Type       | Required    | Notes                                                                                                                                                                                                                                                                 |
| ------------- | ---------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `targetType`  | `String`   | yes         | `WellKnownBanTargetTypes` value: `CHARACTER`, `ACCOUNT`, `IP`, `HWID`, or `HARD` (fan-out marker — the host expands one `HARD` command into the full concrete set — character + account + IP + HWID — for the same subject; a persisted ban row never carries `HARD`) |
| `targetValue` | `String`   | yes         | Keyed datum for `targetType` — char id (as a string), account login, plaintext IP, or HWID hash. For `HARD`, the subject's char id                                                                                                                                    |
| `banType`     | `String`   | yes         | `WellKnownBanTypes` value: `GAME_LOGIN`, `CHAT`, `CHAT_SHADOW`, `PARTY`, `JAIL`                                                                                                                                                                                       |
| `permanent`   | `boolean`  | yes         | Drives expiry: `true` → `expiresAt` MUST be `null`; `false` → `expiresAt` required. Constructor enforces `permanent == (expiresAt == null)`                                                                                                                           |
| `expiresAt`   | `Instant?` | conditional | Instant the ban lapses; `null` iff `permanent`                                                                                                                                                                                                                        |
| `reason`      | `String?`  | no          | Human-readable ban reason, surfaced to the player and stored on the ban row                                                                                                                                                                                           |
| `issuedBy`    | `String?`  | no          | Admin display name or service identifier, stored on the ban row for audit                                                                                                                                                                                             |

**Result** (`BanResult`)

| Field    | Type         | Notes                                                                                                                                                                                                                     |
| -------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `banIds` | `List<Long>` | Ids of the ban rows created or matched. Never `null`; empty when the ban kind is not persisted as an id-bearing row (e.g. a char-variable-backed shadow chat ban). A `HARD` fan-out returns one id per concrete dimension |

**Errors**

| Status              | When                                                                                           |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| `NOT_FOUND`         | The targeted character / account does not exist                                                |
| `VALIDATION_FAILED` | Wire payload missing `targetType`, `targetValue`, or `banType`, or an unrecognized combination |
| `FORBIDDEN`         | Operation rejected on host policy grounds                                                      |

---

### `UnbanCommand`

**Purpose.** Lift a previously applied ban — the inverse of `BanCommand`:
names the same target dimension and ban kind and asks the host to clear
the matching ban(s). Clearing a ban that is not present is a no-op
success (`removed = false`), not an error — the post-condition (no such
ban) already holds.

**Inputs**

| Field         | Type     | Required | Notes                                                                                            |
| ------------- | -------- | -------- | ------------------------------------------------------------------------------------------------ |
| `targetType`  | `String` | yes      | `WellKnownBanTargetTypes` value. A `HARD` target clears every concrete dimension for the subject |
| `targetValue` | `String` | yes      | Keyed datum for `targetType`                                                                     |
| `banType`     | `String` | yes      | `WellKnownBanTypes` value naming which ban kind to clear                                         |

**Result** (`UnbanResult`)

| Field           | Type         | Notes                                                                                                          |
| --------------- | ------------ | -------------------------------------------------------------------------------------------------------------- |
| `removed`       | `boolean`    | `false` when no matching ban existed — a no-op success, not an error                                           |
| `removedBanIds` | `List<Long>` | Ids of the ban rows cleared. Never `null`; empty when nothing matched or the ban kind is not an id-bearing row |

**Errors**

| Status              | When                                                           |
| ------------------- | -------------------------------------------------------------- |
| `VALIDATION_FAILED` | Wire payload missing `targetType`, `targetValue`, or `banType` |
| `FORBIDDEN`         | Operation rejected on host policy grounds                      |

---

## Character commands

### `TransferCharToAccountCommand`

**Purpose.** Re-bind a character to a different account (account migration,
re-sale support). Forces logout if the source player is online so the
target account picks the character up cleanly on next login.

**Inputs**

| Field       | Type     | Required | Notes                          |
| ----------- | -------- | -------- | ------------------------------ |
| `charId`    | `Long`   | yes      | Character to re-bind           |
| `accountTo` | `String` | yes      | Destination login (must exist) |

**Result** (`TransferCharToAccountResult`)

| Field            | Type      | Notes                                          |
| ---------------- | --------- | ---------------------------------------------- |
| `charId`         | `Long`    | Echo                                           |
| `newAccountName` | `String`  | Echo of destination account                    |
| `wasLoggedOut`   | `boolean` | `true` if the player was online and got kicked |

**Errors**

| Status              | When                                                |
| ------------------- | --------------------------------------------------- |
| `NOT_FOUND`         | Character or destination account does not exist     |
| `INVALID_STATE`     | Character cannot be re-bound (clan-leader, jail, …) |
| `VALIDATION_FAILED` | Missing fields                                      |
| `INTERNAL_ERROR`    | Unexpected failure (DB write, etc.)                 |

**Side effects on success.** `requestNow("character", charId)`.

---

### `UpsertCharacterLockCommand`

**Purpose.** Set, replace, or clear a single character lock (IP / HWID /
item-trade). Mirrors the in-game voiced `Security` command, which sets a
lock via `player.setVar(lockVar, value)` and clears it via
`player.setVar(lockVar, 0)`. Exactly one `lockType` is named per call, so a
single command can never accidentally touch any lock other than the one it
targets. Set/clear is naturally idempotent (writes an absolute value, not a
delta) — Kafka redelivery on crash recovery re-applies the same final
state.

**Inputs**

| Field      | Type      | Required | Notes                                                                                                                                        |
| ---------- | --------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `charId`   | `Long`    | yes      | Target character's primary key                                                                                                               |
| `lockType` | `String`  | yes      | `WellKnownCharacterLockTypes` value: `IP`, `HWID`, or `ITEM`                                                                                 |
| `value`    | `String?` | no       | A non-blank value sets/replaces the lock to that value; `null` or blank clears it (host writes the `"0"` sentinel, matching core convention) |

**Result** (`UpsertCharacterLockResult`)

| Field    | Type                 | Notes                                                                                                                                                                                                                                     |
| -------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `charId` | `Long`               | Echo of the target character                                                                                                                                                                                                              |
| `lock`   | `CharacterLockState` | Post-upsert state of the affected lock: `lockType` (`String`), `active` (`boolean` — in effect after the upsert), `value` (`String?` — bound datum: plaintext IP for `IP`, HWID hash for `HWID`/`ITEM`; `null` when cleared or valueless) |

**Errors**

| Status              | When                                                                                     |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `NOT_FOUND`         | Character does not exist                                                                 |
| `VALIDATION_FAILED` | Wire payload missing `charId` or `lockType`, or `lockType` is not a recognized lock kind |
| `FORBIDDEN`         | Operation rejected on host policy grounds                                                |

---

## Gd commands

### `GdResyncCommand`

**Purpose.** Instruct the gd-sync module to re-publish a full snapshot of
every registered game-data entity (itemtemplate, npctemplate, skill,
recipe, armorset, soulcrystal, class, instance). Pure adapter operation —
no host game code involved; the snapshot is the same burst the module
fires on connect / host datapack-reload.

**Inputs** — none. Granularity is always full snapshot (no per-entity
selection).

**Result** (`GdResyncResult`)

| Field              | Type           | Notes                                                                                                                                                                                                  |
| ------------------ | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `acceptedEntities` | `List<String>` | Entity names scheduled for re-snapshot, taken from the live provider registry rather than hardcoded. Never empty on a real ack — an adapter with zero active gd entities replies `UNAVAILABLE` instead |

**Errors**

| Status        | When                                                                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------ |
| `UNAVAILABLE` | The gd-sync module is not active (disabled / failed / no provider on the classpath / no gd topics from `/connect`) |

**Note.** The reply is an ack returned after the snapshot is scheduled —
it does NOT wait for the publish to finish. Completion is observable on
the platform via the per-entity `SNAPSHOT_COMPLETE` markers in
nx-gamedata, not a separate completion event.

---

## Item commands

### `CreateItemCommand`

**Purpose.** Grant a fresh item stack to a character — admin item-grant,
event reward, support compensation.

**Inputs**

| Field            | Type            | Required | Notes                                                                             |
| ---------------- | --------------- | -------- | --------------------------------------------------------------------------------- |
| `charId`         | `Long`          | yes      | Target character primary key                                                      |
| `itemTemplateId` | `Long`          | yes      | Catalog template id (e.g. `57` = adena). NOT a stack object-id                    |
| `count`          | `Long`          | yes      | Quantity to grant. Must be positive                                               |
| `enchantLevel`   | `Long?`         | no       | Default `0`. Applied to non-stackable templates only; ignored for stackable       |
| `location`       | `ItemLocation?` | no       | Default `INVENTORY`. Today supported: `INVENTORY`, `WH`. Others → `INVALID_STATE` |

**Result** (`CreateItemResult`)

| Field          | Type           | Notes                                                                                  |
| -------------- | -------------- | -------------------------------------------------------------------------------------- |
| `itemId`       | `Long`         | Object-id of the resulting stack. For stackable + existing stack → existing stack's id |
| `countCreated` | `Long`         | Echo of `count` (host may clamp on stack-size limits — typically equal to input)       |
| `enchantLevel` | `Long`         | Actually applied enchant (0 for stackable templates regardless of request)             |
| `location`     | `ItemLocation` | Echo of `location` for caller confirmation                                             |

**Errors**

| Status              | When                                                                   |
| ------------------- | ---------------------------------------------------------------------- |
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
| -------- | ------ | -------- | -------------------------------------------------------------- |
| `charId` | `Long` | yes      | Owner character primary key                                    |
| `itemId` | `Long` | yes      | Object-id of the specific stack (NOT template id)              |
| `count`  | `Long` | yes      | Quantity to remove. Must be positive. Clamped to current stack |

**Result** (`DeleteItemResult`)

| Field          | Type      | Notes                                                                                          |
| -------------- | --------- | ---------------------------------------------------------------------------------------------- |
| `itemId`       | `Long`    | Echo of the stack that was decremented / destroyed                                             |
| `countDeleted` | `Long`    | Actual count removed — MAY be less than requested when the live stack was smaller              |
| `fullyDeleted` | `boolean` | `true` if the stack is gone (object destroyed); `false` if it was decremented and still exists |

**Errors**

| Status              | When                                                                       |
| ------------------- | -------------------------------------------------------------------------- |
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
| ------------ | ------ | -------- | ------------------------------------------------ |
| `charIdFrom` | `Long` | yes      | Source character                                 |
| `charIdTo`   | `Long` | yes      | Target character (must differ from `charIdFrom`) |
| `itemId`     | `Long` | yes      | Object-id of the source stack                    |
| `count`      | `Long` | yes      | Quantity to move. Clamped to source stack size   |

**Result** (`TransferItemToCharacterResult`)

| Field              | Type   | Notes                                           |
| ------------------ | ------ | ----------------------------------------------- |
| `itemId`           | `Long` | Echo of the moved stack's id                    |
| `countTransferred` | `Long` | Actual count moved (may be less than requested) |
| `fromCharId`       | `Long` | Echo                                            |
| `toCharId`         | `Long` | Echo                                            |

**Errors**

| Status              | When                                                                             |
| ------------------- | -------------------------------------------------------------------------------- |
| `NOT_FOUND`         | Either character or the item not found                                           |
| `INVALID_STATE`     | Target inventory capacity / weight cap exceeded; item in an unsupported location |
| `VALIDATION_FAILED` | Same-character transfer, missing field, non-positive count                       |
| `INTERNAL_ERROR`    | Unexpected handler failure                                                       |

**Side effects on success.** `requestNow("item", itemId)` and
`requestNow("character", [fromId, toId])`.

---

## Mail commands

### `SendMailCommand`

**Purpose.** Compose and deliver an in-game mail to a character with
optional item attachments. Partial-success aware: some attachments may
fail to materialize (unknown template, capacity exceeded for non-stackable),
the mail itself is still delivered with whatever succeeded.

**Inputs**

| Field    | Type             | Required | Notes                                                  |
| -------- | ---------------- | -------- | ------------------------------------------------------ |
| `charId` | `Long`           | yes      | Recipient character                                    |
| `author` | `String?`        | no       | Display author. Defaults to system character on host   |
| `title`  | `String`         | yes      | Mail subject (≤ 128 chars per L2 wire limit)           |
| `body`   | `String?`        | no       | Mail body (≤ 512 chars per L2 wire limit)              |
| `items`  | `List<MailItem>` | no       | Each `MailItem`: `itemTemplateId: Long`, `count: Long` |

**Result** (`SendMailResult`)

| Field            | Type                      | Notes                                                               |
| ---------------- | ------------------------- | ------------------------------------------------------------------- |
| `createdMailIds` | `List<Long>`              | Host mail-row ids that landed (1 per mail row; usually one entry)   |
| `itemErrors`     | `List<ItemDeliveryError>` | Per-item failures (template id + reason). Empty list = full success |

**Errors**

| Status              | When                                        |
| ------------------- | ------------------------------------------- |
| `NOT_FOUND`         | Recipient character does not exist          |
| `VALIDATION_FAILED` | Missing required field, title too long, ... |
| `INTERNAL_ERROR`    | Mail subsystem rejected the write           |

**Side effects on success.** `requestNow("character", charId)`. Mail
entity is its own sync subject when configured.

---

## Privatestore commands

### `StartPrivateStoreSellCommand`

**Purpose.** Open a regular ("sell one by one") private store on behalf
of a character, listing the given inventory stacks at their asked prices.
Executed by the host's private-store subsystem on the character's game
thread. The host MAY reject individual lines (item no longer in inventory,
not tradeable, …) while still opening the store with the remaining lines —
rejected lines are reported in `dropped` on the success envelope; only
`VALIDATION_FAILED` on the whole command halts the store from opening at
all.

**Inputs**

| Field    | Type             | Required | Notes                                                                                                                                                                                                                                                 |
| -------- | ---------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `charId` | `int`            | yes      | Character to open the store for                                                                                                                                                                                                                       |
| `title`  | `String?`        | no       | Store banner text shown above the seller. `null` falls back to the host's default                                                                                                                                                                     |
| `lines`  | `List<SellLine>` | yes      | Offered stacks, non-empty. Each `SellLine`: `itemId` (`int`, inventory instance object-id), `count` (`long`, positive), `priceAdena` (`long`, non-negative — `0` is a valid give-away price; engine charges `count * priceAdena` for the whole stack) |

**Result** (`StartPrivateStoreResult`)

| Field           | Type                | Notes                                                                                                                                                                                                                                                                                                                                                                              |
| --------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `storeType`     | `String`            | Open-string store-type token the host opened (e.g. `"SELL"` / `"PACKAGE_SELL"`); host-defined vocabulary, not a closed adapter enum                                                                                                                                                                                                                                                |
| `acceptedCount` | `int`               | Number of requested lines the host actually listed                                                                                                                                                                                                                                                                                                                                 |
| `dropped`       | `List<DroppedLine>` | Requested lines the host rejected when opening the store. Non-null; empty when every line was accepted. Each `DroppedLine`: `itemId` (`int`), `reason` (`String`, open `UPPER_SNAKE_CASE` token — known values `NOT_FOUND`, `NOT_TRADEABLE`, `ITEM_BLOCKED`, `EQUIPPED`, `BAD_COUNT`, `PRICE_OVERFLOW`, `REJECTED`; the set is not closed, consumers MUST tolerate unknown tokens) |

**Errors**

| Status              | When                                                                              |
| ------------------- | --------------------------------------------------------------------------------- |
| `NOT_FOUND`         | `charId` does not exist / is not online on this server                            |
| `VALIDATION_FAILED` | `lines` missing/empty, or any `SellLine` entry is malformed                       |
| `INVALID_STATE`     | The character cannot open a store right now (in combat, already trading, dead, …) |

---

### `StartPrivateStorePackageSellCommand`

**Purpose.** Open an all-or-nothing "package sell" private store — a
buyer must purchase every listed line in one transaction, rather than
picking lines individually as with `StartPrivateStoreSellCommand`. Reply,
required/optional fields, and partial-acceptance semantics are identical
to `StartPrivateStoreSellCommand` (source Javadoc states no further
detail beyond that equivalence).

**Inputs**

| Field    | Type             | Required | Notes                                                                                                                |
| -------- | ---------------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| `charId` | `int`            | yes      | Character to open the store for                                                                                      |
| `title`  | `String?`        | no       | Store banner text shown above the seller. `null` falls back to the host's default                                    |
| `lines`  | `List<SellLine>` | yes      | Bundled stacks, non-empty, all-or-nothing at purchase time (same `SellLine` shape as `StartPrivateStoreSellCommand`) |

**Result** (`StartPrivateStoreResult`) — same shape as
`StartPrivateStoreSellCommand`'s result, above.

**Errors** — same as `StartPrivateStoreSellCommand`, above.

---

### `StopPrivateStoreCommand`

**Purpose.** Close whatever private store a character currently has open
(sell, package-sell, or buy). Executed by the host's private-store
subsystem on the character's game thread.

**Inputs**

| Field    | Type  | Required | Notes                               |
| -------- | ----- | -------- | ----------------------------------- |
| `charId` | `int` | yes      | Character whose open store to close |

**Result** (`StopPrivateStoreResult`)

| Field               | Type     | Notes                                                                                                                                          |
| ------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `previousStoreType` | `String` | Open-string store-type token that was open before this command closed it (e.g. `"SELL"` / `"PACKAGE_SELL"` / `"BUY"`); host-defined vocabulary |

**Errors**

| Status          | When                                                   |
| --------------- | ------------------------------------------------------ |
| `NOT_FOUND`     | `charId` does not exist / is not online on this server |
| `INVALID_STATE` | The character has no private store open                |

---

### `BuyFromPrivateStoreCommand`

**Purpose.** Buy the given lots from another character's open sell-store
on behalf of `buyerCharId` — the remote ("buy now") counterpart of the
in-game store purchase packet. The buyer does NOT have to be online, in
range, or in the same world instance as the seller. All-or-nothing: either
every line is bought at exactly the requested count and price, or nothing
is charged and nothing moves — the host validates all lots against the
seller's live trade list before entering the engine, so a stale order book
fails the command instead of silently buying less than the caller saw.
Every non-OK reply also carries a stable machine-readable `reason` code in
`CommandProblem.extensions` (with the numeric context of that reason —
required vs available adena / slots / weight — in sibling extension keys);
the platform localizes the code, the host never sends player-facing text.
The delivery mail's text is the platform's too: `mailSender` / `mailSubject` /
`mailBody` arrive ready-made in the buyer's language and the host writes them
verbatim — it composes no text of its own.

**Inputs**

| Field          | Type            | Required | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| -------------- | --------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `buyerCharId`  | `int`           | yes      | Character paying for the goods. Need not be online — the host loads an offline character for the duration of the deal                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `sellerCharId` | `int`           | yes      | Character whose open sell-store is being bought from. MUST be in the world (online or offline-trading) with a sell-store open. MUST differ from `buyerCharId`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `lines`        | `List<BuyLine>` | yes      | Lots to buy, non-empty, at most `MAX_LINES` = 36 entries, no duplicate `itemId`. Each `BuyLine`: `itemId` (`int`, the instance object-id the buyer saw — primary identity key), `itemTemplateId` (`long`), `enchantLevel` (`Integer?`, `0..127`, `null` if the offer carried none), `attributes` (`Map<Attribute,Integer>`, empty if the offer carried none), `count` (`long`, positive — host buys exactly this many or fails, never silently shrinks), `unitPriceAdena` (`long`, non-negative). Fields beyond `itemId` are an optimistic lock, not a search filter — a mismatch against the live lot fails the whole command rather than buying something else |
| `tax`          | `int`           | yes      | Buyer-side surcharge in whole percent (`5` = 5%) charged on top of the lot price and burned — the seller receives the lot price only. Host clamps to `0..MAX_TAX_PERCENT` (50). Fractional rates unsupported; `0` means no surcharge                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `deadline`     | `Instant`       | yes      | Moment after which the host MUST refuse to execute this command instead of running it — checked before resolving the seller or touching the seller's trade list. Guards against a command sitting in the Kafka backlog while the game-server was down                                                                                                                                                                                                                                                                                                                                                                                                            |
| `mailSender`   | `String`        | yes      | Author shown on the delivery mail, non-blank. Platform-authored, player-facing, already localized; written verbatim                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `mailSubject`  | `String`        | yes      | Subject of the delivery mail, non-blank. Same rules as `mailSender`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `mailBody`     | `String`        | yes      | Body of the delivery mail, non-blank, final text (no placeholders). Same rules as `mailSender`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

**Result** (`BuyFromPrivateStoreResult`)

| Field             | Type               | Notes                                                                                                                                                                                                                           |
| ----------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `itemsTotalAdena` | `long`             | Lot price total credited to the seller                                                                                                                                                                                          |
| `taxAdena`        | `long`             | Burned surcharge, debited from the buyer, credited to nobody                                                                                                                                                                    |
| `paidTotalAdena`  | `long`             | Total debited from the buyer; always `itemsTotalAdena + taxAdena`                                                                                                                                                               |
| `bought`          | `List<BoughtLine>` | The executed lots. Each `BoughtLine`: `itemId`, `itemTemplateId`, `enchantLevel` (`Integer?`), `count`, `unitPriceAdena` (seller price only — the burned surcharge is not included per-line, it is reported once on `taxAdena`) |
| `storeClosed`     | `boolean`          | `true` when this deal emptied the seller's store and the host closed it — the caller drops the whole store from its order book instead of decrementing the bought lots                                                          |
| `mailId`          | `long`             | Id of the delivery mail carrying the bought items; eventual-consistent — resolving it via the mail-read API immediately after this reply may still 404 until mail-ingest catches up                                             |

**Errors**

| Status              | When                                                                                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `VALIDATION_FAILED` | Malformed `lines`, blank mail text, or buyer and seller are the same character                                                                         |
| `NOT_FOUND`         | The seller is not in the world or has no open sell-store                                                                                               |
| `INVALID_STATE`     | The lot no longer matches the request, the store type is not served, or the buyer cannot receive the goods (adena, weight, slots, regulated combat, …) |
| `FORBIDDEN`         | The buyer is barred from trading at all (security lock, cursed weapon, restricted account)                                                             |
| `COMMAND_EXPIRED`   | `deadline` has already passed when the host picked up the command; nothing moved                                                                       |

---

## Sync commands

### `ResyncEntitiesCommand`

**Purpose.** Force the db-sync engine to re-sync whole entities: every
snapshot hash of each targeted entity is invalidated so the next CDC cycle
re-publishes every live row (as `UPDATED`) and re-emits `DELETED` for
snapshot-known ghosts. Pure adapter operation — no host game code
involved. Redelivery re-enqueues the same `resyncId`; the engine merges
pending requests per entity and emits one completion event per drained
`resyncId`, so a duplicate delivery converges to the same platform-visible
outcome.

**Inputs**

| Field      | Type            | Required | Notes                                                                                                                                                                                             |
| ---------- | --------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `resyncId` | `UUID`          | yes      | Platform-generated UUIDv7 identifying the resync operation. Echoed on every `ResyncCompletedEvent` the forced cycles emit                                                                         |
| `entities` | `List<String>?` | no       | Entity names to resync. Null/empty = ALL db-sync entities declared by the adapter's schema provider. A non-empty list containing any unknown name fails the whole command — no partial acceptance |

**Result** (`ResyncEntitiesResult`)

| Field              | Type           | Notes                                                                                                                                                                 |
| ------------------ | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `acceptedEntities` | `List<String>` | Entity names actually enqueued for invalidation — the full declared set when `entities` was omitted, the validated requested set otherwise. Never empty on a real ack |

**Errors**

| Status              | When                                                                                            |
| ------------------- | ----------------------------------------------------------------------------------------------- |
| `VALIDATION_FAILED` | Missing `resyncId`, or any name in a non-empty `entities` list is not a declared db-sync entity |
| `UNAVAILABLE`       | The db-sync engine is not running (module disabled / failed / not started yet)                  |

**Note.** The reply is an ack sent after the invalidation requests are
enqueued — it does NOT wait for the forced cycles. Per-entity completion
is signalled later via `ResyncCompletedEvent` on the `sync` events family.

---

### `ResyncRowsCommand`

**Purpose.** Force the db-sync engine to re-sync selected rows of one
entity: the snapshot hash of each targeted PK is invalidated (a sentinel
entry is inserted for a PK the snapshot never had, so a platform ghost row
gets a `DELETED` re-emit) and the next CDC cycle re-publishes them. Pure
adapter operation. Redelivery merges into the same pending per-entity
invalidation set under the same `resyncId`; the platform sweep keyed on
the completion event is idempotent.

**Inputs**

| Field        | Type         | Required | Notes                                                                                                                                                                                                                                                                                                      |
| ------------ | ------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `resyncId`   | `UUID`       | yes      | Platform-generated UUIDv7 identifying the resync operation                                                                                                                                                                                                                                                 |
| `entityName` | `String`     | yes      | Target entity name as declared by the adapter's schema provider (`EntityMapping.entityName()`)                                                                                                                                                                                                             |
| `pks`        | `List<Long>` | yes      | Primary keys to invalidate, non-empty, at most `MAX_PKS` = 1000 entries. A PK absent from both the snapshot and the host DB still produces a `DELETED` re-emit, repairing platform-side ghost rows                                                                                                         |
| `cascade`    | `boolean`    | no       | Default `false` (Gson primitive default). When `true`, the handler resolves — synchronously, before the ack — the rows of every declared entity whose `parentRefs()` reference `entityName` and invalidates them alongside the requested rows. Cascading from an entity nothing references is not an error |

**Result** (`ResyncRowsResult`)

| Field                 | Type                  | Notes                                                                                                                                                                                                                                                                                                                                                           |
| --------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `invalidatedByEntity` | `Map<String,Integer>` | Per-entity invalidated-row counts: the target entity maps to the number of distinct requested PKs; with `cascade=true` each child entity maps to the number of cascade-resolved rows. Entities that resolved zero cascade rows are omitted (target entity always present). Iteration order: target entity first, cascade children in provider declaration order |

**Errors**

| Status              | When                                                                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `VALIDATION_FAILED` | Missing `resyncId` / `entityName`, unknown `entityName`, or `pks` missing / empty / over `MAX_PKS` / carrying a null or non-positive entry |
| `UNAVAILABLE`       | The db-sync engine is not running                                                                                                          |

**Note.** The reply is an ack sent after enqueue, carrying the per-entity
invalidation counts known at ack time. Per-entity completion follows
asynchronously via `ResyncCompletedEvent`.

---

## Telegram commands

### `TelegramCharLinkCommand`

**Purpose.** Issue a Telegram-to-character link verification code. Sends
the code as an in-game system message to the resolved character; user
enters it back into the Telegram bot to confirm ownership.

**Inputs**

| Field              | Type     | Required | Notes                                                      |
| ------------------ | -------- | -------- | ---------------------------------------------------------- |
| `accountName`      | `String` | yes      | Account login owning the character                         |
| `charName`         | `String` | yes      | Character name (case-insensitive match within the account) |
| `confirmationCode` | `String` | yes      | Caller-generated short code (e.g. 6 alphanumerics)         |
| `telegramUserId`   | `Long`   | yes      | Telegram user-id for audit and uniqueness                  |

**Result** (`TelegramCharLinkResult`)

| Field    | Type   | Notes                                                                      |
| -------- | ------ | -------------------------------------------------------------------------- |
| `charId` | `Long` | Resolved character id (caller stores this against its pending-link record) |

**Errors**

| Status              | When                                                                    |
| ------------------- | ----------------------------------------------------------------------- |
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
| -------------------------------- | ------------------------------------------------- |
| `CreateItemCommand`              | `item` (created stack), `character` (owner)       |
| `DeleteItemCommand`              | `item` (decremented/destroyed stack), `character` |
| `TransferItemToCharacterCommand` | `item` (moved stack), `character` (from + to)     |
| `TransferCharToAccountCommand`   | `character` (re-bound)                            |
| `SendMailCommand`                | `character` (recipient)                           |
| `TelegramCharLinkCommand`        | none                                              |

> The remaining commands added to this catalog (`announcement`, `ban`,
> `UpsertCharacterLockCommand`, `gd`, `privatestore`, `sync`) do not state
> an explicit `requestNow(...)` side effect in their DTO Javadoc, so they
> are intentionally left out of this mapping rather than guessed at.

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

`SendMailCommand` and `StartPrivateStoreSellCommand` /
`StartPrivateStorePackageSellCommand` are the commands with first-class
partial-success semantics: `SendMailResult.itemErrors` enumerates
per-item mail-attachment failures while the mail itself was delivered;
`StartPrivateStoreResult.dropped` enumerates rejected sell lines while the
store still opens with whatever lines were accepted. All other commands
are all-or-nothing — either `OK` with the full effect applied, or a
non-OK status with no state change.
