# Force Resync

## Problem

The platform's mirror of game-server state (`nx-gameservers` `gs_characters` /
`gs_clans` / `gs_alliances` / `gs_items`) can drift from the host DB: a
consumer bug, a lost event, a schema migration mishap, or a manual platform-DB
edit leaves rows stale or orphaned ("ghost" rows that no longer exist on the
game server). The CDC engine cannot repair this on its own — its CRC32 diff
only re-publishes rows whose hash **changed in the host DB**. If the host row
is unchanged but the platform copy is wrong, no cycle will ever re-emit it.

Today the only workaround is: stop the adapter, delete the
`nx-cdc-snapshot/` directory, start the adapter. That forces a cold-start
full sync, but: it requires a restart, it re-emits everything as CREATE
(losing DELETE detection for rows removed while stopped — reopening the
orphan bug snapshot persistence was built to close), and it cannot target a
single character or clan.

This feature adds a platform-issued **force resync**: an operator command
that re-publishes entities (all rows, or selected rows + their dependent
entities) through the normal CDC pipeline, plus a platform-side sweep that
removes ghost rows the adapter cannot know about. After a full resync the
platform DB converges to the host DB state.

Audience: platform operators (new REST commands + resync operations API),
db-sync engine owners (snapshot invalidation), adapter-core owners (new
event family registration), schema-provider authors (new optional
`parentRefs` SPI), nx-gameservers owners (sweep + operation tracking),
nx-tenants / nx-infra owners (topic declaration + provisioning).

## Design summary

Force resync = **snapshot hash invalidation + the existing out-of-band
cycle**. A command handler never re-fetches or re-publishes rows itself; it
perturbs the stored CRC32 of the targeted rows so the next cycle's diff
re-classifies them:

- row exists in host DB → stored hash ≠ computed hash → **UPDATED**
  re-published with full payload;
- row absent from host DB (including a sentinel entry inserted for a PK the
  snapshot never had) → in snapshot, not in scan → **DELETED** re-published.

Because `SnapshotStore` is single-writer-per-entity (mutations happen only
on that entity's cycle), invalidation requests are queued on the engine and
applied on the cycle thread — never from the command handler thread.

Ghost rows on the **platform** that the adapter's snapshot does not contain
cannot be enumerated adapter-side. They are handled by the platform sweep:
every db-sync upsert in nx-gameservers already refreshes `db_synced_at` on
every row (even when values are unchanged) with the adapter-clock event
timestamp, so after a forced cycle every live row carries
`db_synced_at >= cycleStartedAt` and ghosts keep their old stamp. The
adapter emits a `ResyncCompletedEvent` per entity when the forced cycle
fully publishes; the platform then deletes rows whose `db_synced_at`
predates the cycle.

## Requirements

### Adapter — engine (`:nx-gs-db-sync-core`)

**Must:**

- [done] R1. `CdcEngine` MUST expose
  `requestForceResync(UUID resyncId, String entityName)` (whole entity) and
  `requestForceResync(UUID resyncId, String entityName, LongSet pks)`
  (selected rows). Both are thread-safe, non-blocking enqueue +
  `triggerEntityNow`; neither mutates `SnapshotStore` on the calling
  thread. Queue entries carry the `resyncId` so R5 can attribute completion.

- [done] R2. Pending invalidation requests MUST be drained and applied on
  the entity's cycle thread, inside the `ticking` guard, at the top of
  `runGuardedCycle` — BEFORE `task.runCycle()` and therefore before
  `WindowPlanner.plan(...)` reads `snapshot.minPk()/maxPk()` and before
  `bucketByWindows` (which drops PKs outside the planned windows). Applying
  later would defer sentinel DELETEDs by a full cycle. Per-entity merge
  semantics: a whole-entity request absorbs any queued PK sets; PK sets
  union; the drained `resyncId` set is retained until R5 emission.

- [done] R3. Invalidation of a PK present in the snapshot MUST perturb its
  stored CRC to a value that (a) differs from the stored value and (b) is
  never `Phase1Hasher.MISSING_HASH` (`Integer.MIN_VALUE` — the
  `defaultReturnValue` sentinel that `putCrc`/`removeCrc` ExtremeCache
  bookkeeping keys on). Invalidation of a PK absent from the snapshot MUST
  insert a sentinel entry so the diff classifies it as DELETED when the
  host DB has no such row.

- [done] R4. A request arriving while the entity's cycle is mid-flight MUST
  NOT be lost: `runGuardedCycle` re-submits an immediate cycle when the
  pending queue is non-empty at cycle end (the in-flight cycle already
  drained its own batch at start). Today a mid-cycle `triggerEntityNow` is
  a guarded no-op and the change would wait for the next scheduled tick —
  this re-submit is new behavior.

- [done] R5. After the first **fully successful** post-invalidation cycle
  for an entity, the engine MUST emit (via the `NxEvents` facade threaded
  in by `DbSyncModule` — see R12) one `ResyncCompletedEvent` per `resyncId`
  drained into that cycle, carrying `entityName`, `cycleStartedAt`, and
  `completedAt` (adapter clock — `SyncEventPublisher` stamps
  `SyncEvent.timestampEpochMs` from `System.currentTimeMillis()` on the
  same thread, so the platform compares against `db_synced_at` without
  cross-host clock skew). **Fully successful** means: no degraded window
  AND zero failed or still-pending publishes in the cycle. `CycleResult`
  does not carry publish failure counts today (a cycle with 100% failed
  publishes reports HEALTHY) — extending it (or the task) with
  failed/pending publish counts is part of this requirement. On any
  non-successful cycle the emission is deferred: un-acked rows keep their
  perturbed hash (`applyAck` advances per-acked-PK only), the next cycle
  retries publication, and the completion event follows the first fully
  successful cycle (at-least-once; the platform sweep is idempotent).

**Should:**

- [done] R6. A forced full-entity resync SHOULD be logged at INFO with the
  entity name and resyncId — it produces a full re-publication burst
  (every row through Phase 2 + Kafka), which an operator reading logs must
  be able to attribute.

### Adapter — SPI & wire (`:nx-gs-adapter-api`)

**Must:**

- [done] R7. `EntityMapping` MUST gain a default method
  `List<ParentRef> parentRefs()` returning empty (interface default method
  — binary-compatible). `ParentRef { String parentEntityName; String
  fkColumn; }` declares "rows of this entity belong to a row of
  `parentEntityName` via `fkColumn` on this entity's primary table" (e.g.
  item → `("character", "owner_id")`). `fkColumn` MUST pass `SqlIdent`
  validation and `parentEntityName` MUST reference a declared entity —
  both checked in `DbSyncModule.start()` alongside the existing identifier
  validation, failing the module (`STATE_FAILED`) otherwise.

- [done] R8. New command group `kafka.commands.sync`:
    - `ResyncEntitiesCommand implements NxCommand<ResyncEntitiesResult>` —
      `UUID resyncId` (required, platform-generated UUIDv7),
      `List<String> entities` (null/empty = all db-sync entities). Result:
      `List<String> acceptedEntities`. Reply is an **ack**, sent after
      enqueue — it does not wait for cycles.
    - `ResyncRowsCommand implements NxCommand<ResyncRowsResult>` —
      `UUID resyncId`, `String entityName`, `List<Long> pks` (non-empty, max
      1000 — see R13), `boolean cascade`. Result:
      `Map<String, Integer> invalidatedByEntity` (counts known at ack time:
      the requested PKs plus cascade-resolved child PKs).

- [done] R9. New event family `events.sync` with
  `ResyncCompletedEvent { UUID eventId (UUIDv7); UUID resyncId;
  String entityName; Instant cycleStartedAt; Instant completedAt; }`.
  Partition key: `null` (round-robin — low volume, no ordering need).
  Topic: `<tenant>.gs.events.sync`. Declaration and provisioning are
  separate concerns — see R10/R11 and R18.

### Adapter — core (`:nx-gs-adapter-core`)

**Must:**

- [done] R10. `EventTypeRegistry` MUST gain a
  `register(ResyncCompletedEvent.class, "sync", evt -> null)` entry —
  `NxEvents.publish` routes by runtime class and **silently drops** (WARN)
  unregistered types. Without this entry the entire completion → sweep
  chain dead-ends. Requires a `core/vX.Y.Z` release; mind the known
  core/api lockstep hazard when bumping consumers.

- [done] R11. No adapter-core change is needed for the commands: `NxCommands.on(Class, CommandHandler)` wires
  deserialization by `Nx-Message-Type` (= simple class name) for new
  command classes end-to-end, and Gson handles `UUID` natively and
  `Instant` via `NxGsonAdapters`. Stated here so the implementation plan
  doesn't invent a registry change that isn't required.

### Adapter — command handling (`:nx-gs-db-sync-core`)

**Must:**

- [done] R12. `DbSyncModule` MUST register handlers for both commands in
  `onConnect` via `ctx.commands().on(...)` — resync is a pure adapter
  operation, no host code involved — and MUST capture `ctx.events()` and
  thread the facade into `CdcEngine` (constructor change) so R5 can emit.
  A publish before/without the events facade (engine stopped, family
  disabled) follows the existing `NxEvents` no-op semantics.

- [done] R13. Validation: unknown `entityName` / entity not in this
  provider's mappings / empty or oversized `pks` (cap 1000) →
  `VALIDATION_FAILED`; engine not started → `UNAVAILABLE`. For
  `ResyncEntitiesCommand`, any unknown name in a non-empty `entities` list
  → `VALIDATION_FAILED` (no partial acceptance).

- [done] R14. `cascade=true` resolution: for every declared entity whose
  `parentRefs()` reference the target entity, execute
  `SELECT <pkColumn> FROM <primaryTable> WHERE <fkColumn> IN (?, …)`
  (chunked), then `requestForceResync(resyncId, childEntity, resolvedPks)`.
  Resolution runs synchronously in the handler (the reply needs the
  counts; per the `CommandHandler` contract DB I/O on the consumer thread
  is allowed, and the 1000-PK cap bounds the stall). `cascade=true`
  against an entity no one references is not an error — the result simply
  contains only the target entity.

### Platform — nx-tenants & topic provisioning (nx-infra)

**Must:**

- [done] R15. nx-tenants `/connect` response MUST include the `sync`
  family in `messagingTopics.events` (one `Map.entry` in
  `AdapterController`) — a family absent from the map disables publishing
  adapter-side (silent no-op).

- [done] R16. Physical topic provisioning is manual
  (scripts + runbook updated; the `kafka-topics --create` runs for existing
  tenants are an operator action at rollout)
  (`auto.create.topics.enable=false`): add `gs.events.sync` to
  `STANDARD_TOPICS` in `nx-infra` `create-tenant.sh` (prod + dev), create
  `<tenant>.gs.events.sync` for every existing tenant (prod: `bohpts`),
  and update the tenant runbook docs. While touching `STANDARD_TOPICS`,
  fix its known staleness (missing raid / mail / privatetrade / olympiad /
  gameevents / castle families) — do not copy the broken precedent.

### Platform — nx-gameservers

**Must:**

- [done] R17. Two REST endpoints following the existing command-RPC
  pattern (`ServerRequestScope` + `CommandsSender.sendAndAwait` with
  `partitionKey = null` + `gs_command_audits`; `Command` enum +=
  `RESYNC_ENTITIES`, `RESYNC_ROWS`):
    - `POST /gameservers/v1/commands/sync/resync-entities`
      `{entities?: string[]}` → creates a resync operation, sends
      `ResyncEntitiesCommand`, returns `202 {operationId}` on ack.
    - `POST /gameservers/v1/commands/sync/resync-rows`
      `{entityName, pks: long[], cascade: boolean}` → same flow.
    - `GET /gameservers/v1/sync/resync-operations/{id}` → operation status
      with per-entity progress. Lives under `/sync/…`, not `/commands/…` —
      intentional: it reads an operation resource, not a command audit.
      On command timeout / infra error the POST maps through the existing
      handler (504 / 502 `ProblemDetail`) **extended with the `operationId`
      property** so the operator can find the FAILED operation.
      Permissions: `SYNC_RESYNC` for the POSTs, `SYNC_READ` for the GET,
      plus `SYNC_ALL` — seeded in nx-users `v1.2.1_permissions_seed.sql` with
      `ON CONFLICT (name) DO NOTHING`.

- [done] R18. Operation tracking tables (one Liquibase file; name must
  sort after `v2.9.3` under the bare `includeAll` alpha-sort — e.g.
  `v3.0.0_resync_operations.sql`):
    - `gs_resync_operations (id = resyncId UUIDv7, tenant_id, server_id,
    initiator_user_id, status, completed_at)` — `createdAt` derives from
      the UUIDv7 id (platform convention), no column.
    - `gs_resync_operation_entities (operation_id FK, entity_name, kind
    FULL | ROWS, status, cycle_started_at, synced_at, swept_at,
    swept_rows)`.

- [done] R19. Status machines (single source of truth):
    - **Entity:** `PENDING → SYNCED → SWEPT | FAILED`. Entity rows are
      materialized from the ack reply — `acceptedEntities` for
      `ResyncEntitiesCommand` (kind FULL), `invalidatedByEntity.keySet()`
      for `ResyncRowsCommand` (kind ROWS); an ack with an empty list fails
      the operation. `PENDING → SYNCED` on `ResyncCompletedEvent` (R20).
      `SYNCED → SWEPT` by the sweeper (R21). **SYNCED is terminal for
      entities the sweep does not apply to** (kind ROWS target entity, and
      kind ROWS children when no scoped relation is known) — the TTL expirer
      only fails entities still in `PENDING`.
    - **Operation:** `PENDING → IN_PROGRESS → COMPLETED |
    PARTIALLY_FAILED | FAILED`. `PENDING → IN_PROGRESS` on ack (entity
      rows materialized); `→ FAILED` on command timeout/error/empty-ack or
      TTL expiry with zero non-PENDING entities; terminal status is set by
      whichever scheduled job moves the **last** entity to a terminal state:
      all entities terminal-ok → COMPLETED, mixed → PARTIALLY_FAILED, none
      ok → FAILED.
    - The TTL (`nx-gameservers.resync.operation-ttl`, default `PT1H`)
      expires `PENDING` entities of operations whose completion events never
      arrived (adapter died with its in-memory queue) → entity FAILED,
      **never swept**.

- [done] R20. A consumer on `^.*\.gs\.events\.sync$` (per-family pattern +
  `KafkaScopes` `Nx-Server-Id` resolution, like every events consumer)
  marks the matching operation entity SYNCED and records
  `cycle_started_at` from the event. Unknown `resyncId` (e.g. operation
  expired) is logged and dropped.

- [done] R21. A scheduled sweeper (`@Scheduled` + `@Transactional` +
  `@SchedulerLock` — the service runs 2 replicas; the TTL expirer of R19
  is equally lock-gated) executes, for each kind-FULL entity in SYNCED
  whose `synced_at` + grace (`nx-gameservers.resync.sweep-grace`, default
  `PT5M`) has passed:
  `DELETE FROM <table> WHERE tenant_id = ? AND server_id = ?
  AND (db_synced_at < :cycleStartedAt OR db_synced_at IS NULL)` —
  PK-ordered `FOR UPDATE SKIP LOCKED` subquery (lock-order pattern from
  `CharacterOnlineIdleSweeper`), executed in a LIMIT-batched loop until no
  rows remain (the batching is new — the idle sweeper is one-shot; batch
  size knob `nx-gameservers.resync.sweep-batch-size`). The loop re-runs
  until empty, so a row skipped via SKIP LOCKED in one pass is retried —
  the sweep is exhaustive, not best-effort. `db_synced_at IS NULL` covers
  skeleton rows created by runtime-only upserts. Children cascade via FK
  (`gs_character_subclasses`, `gs_clan_skills`, `gs_item_attributes`;
  note `gs_items.owner_id` has NO FK to characters — items are swept as
  their own entity). Entity→table mapping (`character`→`gs_characters`,
  `clan`→`gs_clans`, `alliance`→`gs_alliances`, `item`→`gs_items`) is
  platform knowledge and lives in the sweeper.

- [done] R22. For kind-ROWS operations with `cascade=true` and a
  platform-known scoped relation (today: target `character` → child
  `gs_items` via `owner_id`), the sweeper additionally executes a scoped
  sweep gated on the **child** entity row: it runs only once the `item`
  entity is SYNCED + grace, and uses the **item** entity's
  `cycle_started_at` as cutoff (using the character's earlier timestamp
  would under-sweep; running before item SYNCED would mass-delete live
  items not yet re-consumed):
  `DELETE FROM gs_items WHERE … AND owner_id IN (:pks) AND
  (db_synced_at < :cycleStartedAt OR db_synced_at IS NULL)`.
  The target-entity PKs themselves need no sweep: a requested ghost PK is
  covered adapter-side by the R3 sentinel → DELETED event.

**Should:**

- [done] R23. The sweep is **self-healing by construction** and the spec
  documents it: a row deleted prematurely (consumer lag exceeding grace)
  is re-created by its late UPDATED event (`INSERT … ON CONFLICT`),
  because every live host row was re-emitted by the forced cycle. Ghost
  rows never receive an event and stay deleted. Grace bounds churn, not
  correctness. Per-PK Kafka keying additionally prevents a redelivered
  older event from regressing `db_synced_at` past a newer one within a
  partition.

### Non-goals

- **Resync of runtime-sync and gd-sync engines.** db-sync only. Runtime
  state is volatile and re-emitted continuously; game-data resync is an
  existing separate concern (host restart / re-ingest).

- **Synchronous completion replies.** Commands ack on enqueue. Progress is
  observable via the resync-operations API, heartbeat entity stats, and the
  sync streams themselves.

- **Crash-durable resync requests.** The pending invalidation queue and
  in-flight resyncId set are in-memory. Adapter crash or reconnect between
  ack and the completing cycle drops them; the platform operation expires
  to FAILED and the operator re-issues. No persistence of pending requests.

- **Throttling / scheduling of the re-publication burst.** A full items
  resync re-fetches and re-publishes every row (millions of events; the
  cycle runs far longer than a normal tick). This is the same load shape as
  the existing cold-start initial sync and rides the same chunked Phase 2 +
  bounded publisher path. Operators choose the timing; the engine does not
  rate-limit beyond existing back-pressure.

- **An operations list endpoint.** `GET …/resync-operations/{id}` only;
  the POST error path returns the `operationId`, and `gs_command_audits`
  already lists issued commands. Add a list endpoint when a real need
  appears.

- **Clearing the snapshot as the resync mechanism.** Rejected:
  `clearEntity()` re-emits everything as CREATED but loses DELETE detection
  (rows deleted from the host DB during the resync window, and snapshot-known
  ghosts) — strictly worse than hash perturbation.

- **Out-of-band fetch+publish in the command handler.** Rejected: a second
  publish path racing the cycle's per-row snapshot swap, for latency nobody
  needs (the triggered cycle starts within seconds).

### Edge cases

- **Request arrives mid-cycle** → trigger is a guarded no-op, but R4's
  end-of-cycle re-submit runs the invalidations in the immediately following
  cycle.
- **Concurrent resyncs of the same entity** → requests merge (R2); every
  drained `resyncId` gets its own completion event off the same fully
  successful cycle.
- **Perturbed CRC accidentally equals the freshly computed CRC** → only
  possible if the row changed in the host DB during the window (real change
  → diff fires anyway via the changed columns on the next natural edit;
  probability 2⁻³²). For unchanged rows the flip guarantees mismatch.
- **Sentinel insert for a PK that actually exists in the host DB** → Phase 1
  computes the real hash, sentinel ≠ real → UPDATED. Harmless either way.
- **Sentinel PK outside the current window envelope** → `WindowPlanner`
  envelopes the union of DB range and snapshot range (`unionMin/unionMax`);
  R2's drain-before-plan ordering guarantees inserted sentinels extend the
  snapshot range before planning and are scanned.
- **Disconnect/reconnect between ack and cycle** → engine stops
  (`flushAll` + `clearAll`), pending queue and in-flight resyncIds drop,
  snapshot reloads from disk on reconnect. Operation expires to FAILED
  (R19). Re-issue.
- **Snapshot checkpoint during a forced cycle** → impossible mid-cycle:
  checkpoint runs after `runCycle()` returns, by which time published rows
  carry real CRCs and DELETED sentinels are removed. Perturbed values can
  reach disk only if the cycle failed before publishing them — and then
  they are exactly what makes the retry re-emit after a restart.
- **`ResyncRowsCommand` with `pks` > 1000** → `VALIDATION_FAILED`; the
  operator batches, or uses a full-entity resync.
- **Character rows created by runtime-sync only (`db_synced_at IS NULL`)**
  → swept; if the character exists on the host its forced-cycle UPDATED (or
  next runtime tick) re-creates the row. Covered by R23 self-healing.
- **Completion event consumed before all data events (cross-topic lag)** →
  grace delay absorbs typical lag; beyond it, R23 self-healing applies.
- **Sweeper down / grace elapsed long ago** → sweep executes whenever the
  sweeper resumes; the cutoff predicate is time-anchored and idempotent.
- **Two platform replicas** → sweeper and TTL expirer are
  `@SchedulerLock`-gated (R21/R19); the SYNCED→SWEPT transition runs under
  the lock, so `swept_rows` accounting cannot double-run.

## Open questions

- [assumed: `pks` cap of 1000 per `ResyncRowsCommand` keeps the command
  record well under Kafka's default 1 MB and bounds the cascade `IN`-list
  fan-out; larger repairs use full-entity resync.]
- [assumed: sweep grace default `PT5M` — items consumer lag after a 12M-row
  re-publication burst is expected in the low minutes; operators tune via
  `nx-gameservers.resync.sweep-grace`.]
- [assumed: operation TTL default `PT1H` — generous against the longest
  realistic forced items cycle, small against operator patience.]
- [assumed: completion events ride a new `sync` event family rather than an
  existing one — unlike `LevelExpTableSnapshotEvent` (which piggybacked the
  `character` family), resync completion spans all db entities and has no
  semantically adjacent family to borrow.]
- [assumed: per-client schema providers (bohpts) declare `parentRefs` for
  item → character in their own repos; this repo's vanilla modules only
  ship the SPI.]

## Links

- CDC engine internals (cycle, windows, two-phase protocol):
  [`docs/features/cdc-engine/spec.md`](../cdc-engine/spec.md) — this
  feature uses the inbound command channel that cdc-engine R14 anticipated
  (R14 itself — dynamic per-entity config overrides — remains open).
- Snapshot store & persistence (single-writer contract, checkpoint timing):
  [`docs/features/snapshot-persistence/spec.md`](../snapshot-persistence/spec.md).
- Inbound commands RPC (handler SPI, reply envelope, error codes):
  [`docs/features/commands/spec.md`](../commands/spec.md).
- Platform command bridge (`CommandsSender`, audits) — nx-gameservers repo,
  `infra/kafka/CommandsSender.java`.
