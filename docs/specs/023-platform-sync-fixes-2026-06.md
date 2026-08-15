# Platform sync fixes — 2026-06 batch

Status: design / approved-in-shape
Date: 2026-06-29
Scope: three independent fixes across `nx-gs-adapter`, `nx-gameservers`,
`nx-gamedata`, and `bohpts-core`, specified together but implementable in
parallel.

| #   | Fix                                                  | Repos touched                                                             |
| --- | ---------------------------------------------------- | ------------------------------------------------------------------------- |
| ①   | Targeted-window force-resync (fast per-PK republish) | `nx-gs-adapter` (`nx-gs-db-sync-core`)                                    |
| ②   | Upsert character-locks command (IP / HWID / ITEM)    | `nx-gs-adapter-api`, `nx-gameservers`, `nx-users` (seed), `bohpts-core`   |
| ③   | Gearscore gd-sync deploy gap                         | `nx-gs-adapter` (release), `bohpts-core` (dep bump), `nx-gamedata` (test) |

---

## Fix ① — Targeted-window force-resync

> Living spec for the feature: [021-force-resync.md](021-force-resync.md), which already carries the
> shipped windowing behaviour. This section is the 2026-06 change record.

### Problem (confirmed)

After an item transfer (or any per-PK resync — item create/delete, mail
delivery, lock change) the moved rows do not appear on the platform until the
next scheduled 60s CDC tick, even though the resync path fires correctly.

Root cause is **not** the trigger wiring and **not** a DB-commit race on the
reference server (`x7/live` runs `LazyItemsUpdate=False` → online item writes
are synchronous). The cause is cycle cost:

- `NxSync.requestResync` → `DbSyncModule.handleNxSyncResync` →
  `CdcEngine.requestPkRepublishNoEvent` → `triggerEntityNow` → immediate cycle.
  All wired and deployed (release HEAD `TransferItemToCharacterHandler:74`,
  `ItemMapping.parentRefs()` declares `("character","owner_id")`).
- `ResyncCoordinator.drainAndInvalidate` only perturbs the snapshot CRC for the
  invalidated PKs (`snapshot.invalidate(entity, pk)`). It does **not** narrow
  the scan.
- `EntitySyncTask.runCycle` therefore plans windows over the entity's whole PK
  envelope and Phase-1 CRC-hashes every row in every window. On a live `items`
  table (millions of rows) one cycle ≈ the tick interval, and the per-entity
  `ticking` guard coalesces the resync behind any in-flight scheduled cycle. Net
  effect: the targeted republish lands roughly when the next scheduled tick
  would have published it anyway.

### Design

Add a **targeted fast-path** to `nx-gs-db-sync-core`: when a cycle is driven
solely by a bounded set of explicitly-invalidated PKs (no whole-entity `all`
pending and no scheduled-full reason), Phase-1 hashes only those PKs via a
chunked `WHERE pk IN (...)` query instead of a full-table scan.

Components:

- **`ResyncCoordinator`** — `drainAndInvalidate` already separates `all`,
  tracked `pks`, and `noEventPks`. Return the drained targeted PK set (union of
  `pks` + `noEventPks`) to the caller, and a flag `targetedOnly` = true when
  `all == false`. When `all` is pending, `targetedOnly = false` (full scan
  required — invalidate-all must re-scan everything).
- **`CdcEngine.runGuardedCycle`** — distinguish a _scheduled_ tick from a
  _triggered_ (out-of-band) run. Only a triggered run with a non-empty targeted
  set and `targetedOnly == true` takes the fast path. A scheduled tick always
  runs the full scan (it must catch external deletes/changes the resync set does
  not know about).
- **`WindowPlanner`** — add `planTargeted(mapping, conn, pkSet)` returning a
  small number of windows each backed by an explicit PK `IN`-list (chunk width
  reuses the existing cascade chunk size, 500). No min/max range scan.
- **`Phase1Hasher`** — support an `IN`-list primary/child hash query scoped to
  the targeted PKs (children scoped by `fk IN (...)` over the targeted parent
  PKs).
- **`EntitySyncTask.runCycle`** — accept the targeted PK set; when present, plan
  via `planTargeted` and hash only those rows. Diff, Phase-2 fetch, publish, and
  snapshot advance are unchanged (already per-PK). Ghost PKs (resync of a
  deleted row) naturally fall out as DELETE: the `IN`-list returns no row, the
  perturbed snapshot entry has no match → DELETE published.

### Edge cases

- A targeted resync racing a scheduled full cycle: the `ticking` guard +
  `pendingImmediate`/`hasPending` re-submit logic is preserved; a coalesced
  re-run re-reads the still-pending targeted set. If a whole-entity `all` request
  arrives, it absorbs the targeted set and forces the full path.
- `ResyncCompletedEvent` emission gate (`onCycleResult`) is unchanged — a
  targeted cycle that fully succeeds still emits completion for tracked
  resyncIds.
- Snapshot PK-envelope semantics: targeted path must not shrink the stored
  snapshot; it only reads/diffs the targeted subset and advances those entries.

### Tests (`nx-gs-db-sync-core`)

- `runCycle_shouldHashOnlyTargetedRows_whenTriggeredWithBoundedPkSet` —
  assert Phase-1 issued an `IN`-list query, not a full-range scan, and only the
  changed PK published.
- `runCycle_shouldFullScan_whenScheduledTick` — scheduled path unchanged.
- `runCycle_shouldFullScan_whenWholeEntityResyncPending` — `all` forces full.
- `runCycle_shouldPublishDelete_whenTargetedGhostPk` — ghost PK → DELETE.
- Children scoping test — child hash scoped by `fk IN (...)`.

### Out of scope

The secondary `LazyItemsUpdate=True` commit race (destination item flushed via
`targetitem.updateDatabase()` without `force` in `ItemContainer.transferItem`)
is **not** addressed here — `x7/live` has lazy updates off. If a high-rate
server later enables lazy items, a follow-up forces `updateDatabase(true)` on the
destination in `CharacterTransferService` online paths.

---

## Fix ② — Upsert character-locks command

> Current contracts: the lock wire shape lives in
> [013-character-core-extension.md](013-character-core-extension.md), the command's request/reply
> contract in [009-commands/catalog.md](009-commands/catalog.md). This section is the change record.

### Problem

Operators cannot set, change, or clear a character's IP / HWID / ITEM lock from
the platform. Locks live in the game DB `character_variables` as `lockIp` /
`lockHwid` / `lockItem`; a lock is active when its value is non-blank and not the
`"0"` sentinel (`CharacterMapping.isActiveLock`). The in-game voiced command
`Security` already sets (`player.setVar("lockIp", ip)`) and clears
(`player.setVar("lockIp", 0)`) them — we expose the same as a platform command.

### Design — one upsert endpoint, one lock per call

Per the agreed shape: **one lock type per command** (no list), so a single call
can never accidentally touch more than the lock it names. Each call sets/replaces
that one lock to a value, or clears it. To change several locks, the operator
issues several calls.

**`nx-gs-adapter-api`** (`kafka.commands.character`):

- `UpsertCharacterLockCommand implements NxCommand<UpsertCharacterLockResult>`
  - `Long charId`
  - `String lockType` — one of `WellKnownCharacterLockTypes` (`IP`/`HWID`/`ITEM`)
  - `@Nullable String value` — non-blank ⇒ set/replace to this value; null/blank
    ⇒ clear (host writes the `"0"` sentinel, matching the core convention)
  - hand-written builder, Javadoc on the wire contract (Java 8 POJO).
- `UpsertCharacterLockResult`
  - `Long charId`
  - `CharacterLockState lock` — final state of the affected lock (type + active
    - value), so the caller sees the post-upsert truth for that one lock.

**`nx-gameservers`** (`api/rest/commands`):

- `CommandsController` → `POST /gameservers/v1/commands/characters/lock`
  - `@RequirePermission(Permissions.GS_COMMANDS_CHARACTER_LOCK_UPDATE)`
  - `@Operation.summary` prefixed `❗` (mutates a character's access state); the
    OpenAPI customizer mirrors `❗` into the permission block.
  - Request DTO `UpsertCharacterLockRequest { Long charId, String lockType, String value }`
    with validation (`charId` required; `lockType` ∈ enum, required; `value`
    optional — absent/blank = clear).
  - Thin: map → command → `sender.sendAndAwait(...)` → response.
- `Permissions` — add leaf `GS_COMMANDS_CHARACTER_LOCK_UPDATE` under the
  `GS_COMMANDS` domain (covered by `GS_COMMANDS_ALL`).

**`nx-users`** — seed the permission as a new `--changeset` in
`v1.2.1_permissions_seed.sql` (id `users:1.2.1-gs-commands-character-lock`),
`ON CONFLICT (name) DO NOTHING`, description starting with `❗` (mutates the
game world / access) in each locale.

**`bohpts-core`** (`commands/character` — new package):

- `UpsertCharacterLockHandler implements CommandHandler<UpsertCharacterLockCommand, UpsertCharacterLockResult>`
  - validate `charId` (in-int-range) + `lockType` (known); resolve the var name
    (`lockIp`/`lockHwid`/`lockItem`);
  - resolve online vs offline (`GameObjectsStorage.getPlayer`);
  - online → `ctx.host().sync(...)`: `player.setVar(varName, value)` (set) or
    `player.setVar(varName, 0)` (clear);
  - offline → `ctx.io()`: direct JDBC upsert/delete on `character_variables`
    for that single `(charId, varName)` row (set value, or set `"0"`),
    mirroring `ItemPersistence`;
  - on success `ctx.sync().requestResync("character", [charId], false)` (locks
    are character-variable children of the `character` entity — no item cascade).
    With Fix ① this resync is fast.
  - return `UpsertCharacterLockResult` with the affected lock's final state.
- Register in `BohptsCommandsModule.onConnect`.

### Tests

- `nx-gameservers`: controller maps request → command, permission gate, response
  mapping (`@Nested UpsertLock`).
- `bohpts-core`: handler validation, online set/clear path, offline JDBC path,
  resync trigger, and that only the named lock var is touched (per the
  `DeleteItemHandler` test pattern).

### API changelog

Public API change (new endpoint) → produce a front-end changelog at
`nx-gameservers/docs/specs/020-character-lock-update-api-changelog.md`
and surface it in the session.

---

## Fix ③ — Gearscore gd-sync deploy gap

> Feature spec for the module itself: [030-gamedata-sync.md](030-gamedata-sync.md). This section
> records a version-skew incident, not the gd-sync design.

### Root cause (confirmed)

`gd_gearscore_rulesets` is empty while every other `gd_*` table is populated; the
topic `bohpts.gd.sync.gearscore` exists but has **0 messages ever published**
(offsets `0:0`). Pipeline, consumer, and topic provisioning all work
(prod `nx-tenants 1.22.0` advertises the gearscore topic in `/connect`;
`bohpts-core` ships the provider + `META-INF/services` registration; `api 0.64.0`
carries the SPI + DTOs).

The gap is a **version skew on the runtime gd-sync module**: `bohpts-core` pins
`nx-gs-gd-sync-core:0.9.0`, but the gearscore descriptor in
`GameDataSyncModule.defaultDescriptors()` landed in commit `7431553`
(2026-06-22), which **no `gd-sync/*` tag contains** (latest is `gd-sync/v0.9.0`).
So the deployed `GameDataSyncModule` never looks up `GearScoreRulesetProvider`,
never resolves the bohpts provider, never creates the entity → never publishes.

### Steps

1. **Release `nx-gs-gd-sync-core` 0.10.0** — tag `gd-sync/v0.10.0` from current
   `nx-gs-adapter` main (contains `7431553`). Verify gd-sync-core main compiles
   against the api version it consumes and that the gearscore descriptor is
   present in `defaultDescriptors()`. Confirm Maven Central publish before the
   bump.
2. **Bump `bohpts-core`** — `core/build.gradle`:
   `nx-gs-gd-sync-core:0.9.0 → 0.10.0`. No coupled bumps required: `api 0.64.0`
   already carries the gearscore SPI/DTO, `db-sync 0.8.0` already contains
   `7431553`'s db-sync part, `core 0.31.0` is unaffected. Rebuild + redeploy
   bohpts-core (client game host — redeploy is on the client side).
3. **Regression test (`nx-gamedata`)** — add the missing
   `GearScoreSyncIngestIntegrationTest` (UPSERT + SNAPSHOT_COMPLETE → one
   `gd_gearscore_rulesets` row), matching `ItemTemplateSyncIngestIntegrationTest`.

### Validation

After redeploy: the gearscore topic receives at least a `SNAPSHOT_COMPLETE`
marker (≥1 message), and `gd_gearscore_rulesets` gets one row per server **iff
gear score is enabled** in the datapack (`GearScoreConfig.isEnabled()`). If gear
score is disabled on a server, an empty (count=0) snapshot is correct and the
table stays empty for that server — confirm the enabled flag in the datapack
before treating an empty table as a failure.

---

## Rollout / ordering

Three independent workstreams; recommended order by impact/effort:

1. **③ Gearscore** — release `gd-sync/v0.10.0`, bump bohpts dep, add test.
   Smallest change, unblocks the wiki gearscore tables.
2. **② Upsert locks** — new command across api/platform/host + perm seed.
3. **① Targeted-window resync** — engine change in `nx-gs-db-sync-core`;
   benefits every per-PK resync (transfers, item create/delete, mail, and ②'s
   lock resync).

Release/deploy mechanics (tags, image bumps, Komodo auto-deploy, no commit/push
without explicit approval) follow the standing platform rules.
