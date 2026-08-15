# Runtime Sync

## Problem

Game-server cores hold a large amount of player state in JVM memory and flush it to MySQL only
periodically (on logout, on save-tick, on shutdown). Volatile fields — current HP/MP/CP, position
(x/y/z), vitality, autofarm flags — never reach the DB in real time, so the existing `db-sync`
CDC pipeline (CRC32 hashing over `GRANT SELECT` reads) cannot surface them. The L2NX platform's
operator dashboards and player-facing live UIs need this state to be live and accurate within
seconds, not minutes-to-hours.

This slice introduces `runtime-sync`: a sibling adapter module to `db-sync` that pulls volatile
state directly from the host's in-memory game stores via a Tier-2 SPI, computes a Java-side
hash-and-diff per tick, and publishes deltas to per-entity Kafka topics. MVP target: a single
`character` runtime entity (vitals + position) end-to-end against bohpts. Audience: operators
(drop in `nx-gs-runtime-sync-core` JAR alongside `db-sync`), platform-side consumers of
`gs.sync.runtime.*` topics (live dashboards, in-game widgets), future runtime entities (party
state, siege participants, raid boss vitals).

## Requirements

> **Sibling features carry the SPI plumbing and topic delivery contract:**
>
> - Tier-1 SPI (`AdapterModule` + ServiceLoader wire-up) lives in > [`adapter-modules`](002-adapter-modules/spec.md). `runtime-sync` is one more module on the > same plane as `db-sync`.
> - Namespaced `syncTopics` (`db` / `runtime` / `dp`) and `heartbeatTopic`-at-root reorg lives > in [`adapter-bootstrap` R17](001-adapter-bootstrap.md). The runtime engine reads its > topic from `ctx.syncTopics().runtime()` keyed by `entityName()`.
> - The hash-and-diff algorithm shape borrows from > [`cdc-engine`](005-cdc-engine/spec.md) (per-PK hash, snapshot swap on Kafka ack), but > the implementation is independent: no DB phases, no `BIT_XOR(CRC32(...))` SQL, no > sliding-window scans. Snapshot source is `Iterable<RuntimeRow>` from the SPI; hash is > FNV-1a 64-bit computed in Java over typed fields.
>
> All requirements below assume `adapter-bootstrap` R17 has shipped and `nx-gs-adapter-api`
> is on `0.11.0`.

**Must:**

- [done] R1. `nx-gs-runtime-sync-core` MUST implement
  `app.l2nx.gs.adapter.api.spi.AdapterModule` and ship a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` descriptor pointing to its
  module class. Discovery is zero-config — adding the JAR to the host classpath alongside
  `nx-gs-adapter-core` is the only step required to enable the module.
  - SC1. `AdapterModule.name()` returns the literal string `"runtime-sync"` — surfaced in
    `HeartbeatEvent.enabledModules`. Distinct from `"db-sync"`; the two modules coexist.

- [done] R2. `nx-gs-runtime-sync-core` MUST consume the Tier-2 SPI `RuntimeStateProvider`
  (defined in `nx-gs-adapter-api` package `app.l2nx.gs.adapter.api.spi`, alongside Tier-1
  `AdapterModule`, Tier-2 `DbSchemaProvider`, Tier-3 `JdbcConnectionSource`) and discover
  impls via `ServiceLoader.load(RuntimeStateProvider.class)` once at module `start()`.
  Selection rule:
  - **0 impls** → log actionable WARN, module transitions to `DISABLED` (other modules
    and the host JVM keep running)
  - **1 impl** → engine uses it (the dominant case)
  - **>1 impls** → log actionable ERROR listing the conflicting impl class names; module
    transitions to `FAILED`. MVP assumes operator's classpath has exactly one activated
    descriptor.

- [done] R3. `RuntimeStateProvider` interface MUST expose:
  - `String schemaName()` — informational identifier (e.g. `"bohpts"`, `"l2j"`); not a
    selection key in MVP
  - `List<RuntimeEntityMapping<?>> mappings()` — the runtime entities this provider
    surfaces

- [done] R4. `RuntimeEntityMapping<T>` interface MUST describe ONE runtime entity and
  expose:
  - `String entityName()` — domain identifier in singular form (`"character"`,
    `"party"`, …). MUST be non-null, non-blank, and unique across all mappings
    returned by `RuntimeStateProvider.mappings()` — duplicates cause the module
    to transition to `STATE_FAILED` at engine start with an actionable ERROR
    listing the offending names. Used as the lookup key into
    `ConnectResponse.syncTopics.runtime` to resolve the Kafka topic. Surfaced
    through heartbeat as `EntityStats.name`. **Entity name MAY collide with a
    `db-sync` entity of the same name** — namespace separation in `syncTopics`
    (`db.character` vs `runtime.character`) is sufficient.
  - `Class<T> dtoType()` — DTO class for serialization. Concrete, non-parameterized
    (Gson serializes the typed payload slot directly).
  - `Iterable<RuntimeRow<T>> snapshot()` — produces a one-shot snapshot of the
    currently-live entities. Called once per tick on the engine's daemon thread.
    Implementation MUST NOT block on locks held by hot game-server threads; the
    iterable's `next()` MUST be cheap (read field values from the live game object,
    not recompute / refetch). The engine iterates fully and then releases the
    iterable before tick processing — implementation can return a defensive copy
    (e.g. `new ArrayList<>(World.getAllPlayers())`) if the underlying collection
    is concurrently mutated.
  - `RuntimeRow<T>` is a lightweight value: `{ long pk, T dto }`. The engine reads
    `pk` as the entity identity, computes a 64-bit FNV-1a hash over `dto`'s
    hash-relevant fields (declared via the mapping), and uses the resulting hash
    to detect "changed" rows.
  - `long hash(T dto)` — implementation-supplied hash function that returns a stable
    64-bit FNV-1a hash over the fields the operator wants to track for change
    detection. Provider author controls which fields participate (and which are
    ignored — e.g. high-frequency micro-jitter on coordinates can be quantized
    before hashing). Engine treats the hash as opaque.

- [done] R5. The runtime engine MUST schedule one tick task per declared
  `RuntimeEntityMapping` onto a shared bounded thread pool, ticking at a
  configurable interval. Per-tick semantics:
  1. Call `mapping.snapshot()` → `Iterable<RuntimeRow<T>>`. The iteration is
     wrapped in `try/catch (Throwable)` — a misbehaving provider (e.g.
     `ConcurrentModificationException` from a live map view) results in
     `CycleResult.degraded(elapsed)` + WARN log; the scheduler task survives
     and the entity transitions to `DEGRADED` until the next clean tick.
  2. For each row: compute `mapping.hash(row.dto)`, store in a fresh
     `Long2LongOpenHashMap<pk, hash>` (fastutil) whose `defaultReturnValue`
     is `Long.MIN_VALUE` (the `MISSING_HASH` sentinel — removes the
     hash=0 / absent ambiguity that arose when a provider legitimately
     returned `0L` from `mapping.hash(dto)`)
  3. Diff against the previous tick's snapshot:
     - **NEW** (pk in current, not in prev — `prev.get(pk) == MISSING_HASH`)
       → publish `SyncEvent.create(pk, dto)`
     - **CHANGED** (pk in both, hash differs) → publish `SyncEvent.update(pk, dto)`
     - **GONE** (pk in prev, not in current) → **silently drop** from snapshot
       tracking. **No tombstone is emitted.** `db-sync` owns "this entity
       permanently no longer exists" semantics; runtime "absence" is transient
       (logout / disconnect / zone change) and the previous published value is
       treated by platform consumers as the last-known live state.
  4. Walk in-flight futures — done futures are drained first, then deadline-bounded
     wait for pending (parity with `db-sync` to prevent head-of-line block on a
     single slow ack). Ack-walk respects thread interrupt and breaks early on
     shutdown.
  5. Replace prev = current ONLY for pks whose publish was acked (Kafka producer
     callback succeeded). Failed/timed-out publishes leave the previous hash in
     place — replayed on the next tick. Same at-least-once semantics as
     [`cdc-engine`](005-cdc-engine/spec.md). `failedAcks` / `timedOutAcks` counters
     are tallied per cycle and surfaced through `EntityStatsTracker` so Kafka
     outages are visible on the heartbeat per-entity slot.
  6. `CycleResult` reports `DEGRADED` (not `HEALTHY`) when any publish future
     failed or timed out — the prior behaviour of always reporting `HEALTHY`
     regardless of Kafka health hid platform-side outages.
  - SC2. Tick interval default = 10s. Override via
    `l2nx.runtime-sync.tick-interval-seconds`. Per-entity granularity is NOT in
    MVP — single global value applies to every mapping.
  - SC3. Engine end-of-cycle flush wait is configured via
    `l2nx.runtime-sync.publish-flush-seconds` (default 5s). **Per-module key**
    — separate from `l2nx.cdc-engine.publish-flush-seconds` because the two
    engines have different tick cadences and platform-side latency budgets;
    sharing one knob would force operators to compromise. Unacked pks roll
    over to next tick.
  - SC5. Worker pool size is configured via `l2nx.runtime-sync.workers` (default
    `max(2, min(entities, cores/2))`). A `ScheduledThreadPoolExecutor` with
    `nx-runtime-sync-pool-N` daemon threads + an uncaughtExceptionHandler runs
    every entity's tick task; `setRemoveOnCancelPolicy(true)` keeps the work
    queue clean on entity teardown. A per-entity `AtomicBoolean ticking` guard
    causes overlapping ticks (when a cycle exceeds the tick interval) to be
    skipped with a WARN rather than queued, preventing pool exhaustion under
    back-pressure. Replaces the legacy thread-per-entity model.
  - SC6. Engine `stop()` budget for `awaitTermination` is
    `max(2, publishFlushSeconds + 1)` seconds — the prior fixed 2s was too
    short to drain in-flight publishes once `publishFlushSeconds` grew, racing
    shutdown against tail latency.

- [done] R6. The runtime engine treats `mapping.hash(dto)` return value as **opaque** —
  it is compared as a long for "changed" detection only; the algorithm is the
  provider's choice. The recommended default is **FNV-1a 64-bit** via
  `app.l2nx.gs.commons.hash.Fnv1a64` (published in `:nx-gs-commons`): faster than
  CRC32 in pure Java (mul+xor vs table lookup), 64-bit collision safety (~0% on
  10k entries vs ~1% for 32-bit CRC), structurally hashes typed fields without
  intermediate string serialization. Tenants choosing different algorithms are
  free to do so — engine never inspects the hash beyond `==` comparison. (The
  Java-side choice differs from `db-sync`, which uses MySQL-native CRC32 because
  the hash runs on the DB side.)

- [done] R7. The runtime engine MUST resolve the Kafka topic for each entity from
  `ctx.syncTopics().runtime().get(entityName)` — namespaced shape per
  [`adapter-bootstrap` R17](001-adapter-bootstrap.md). Per-entity behavior:
  - Topic missing for the entity → log actionable WARN (`"no runtime topic for
entity '<name>'"`), entity transitions to `DEGRADED`, no publishes for that
    entity. Other entities continue ticking.
  - Topic present → engine publishes `SyncEvent<T>` to that topic, key =
    `LongSerializer.serialize(pk)` (8 bytes, same as db-sync).
  - `null` or empty `runtime` namespace map → module transitions to `DISABLED`
    with an actionable WARN; no engine instantiated, no scheduler threads
    started.

- [done] R8. The runtime engine MUST NOT propagate exceptions to host-JVM threads. Every
  entry point (scheduler tick, `mapping.snapshot()` call, `mapping.hash()` call, Kafka
  producer callback) catches `Throwable`, logs via `NxLog`, and transitions the
  **affected entity** to `DEGRADED` — other entities continue ticking. Module-level
  `FAILED` is reserved for non-recoverable startup conditions (0 / >1
  `RuntimeStateProvider`). Same isolation contract as `db-sync` R9.

- [done] R9. Bohpts client + character runtime MVP — `bohpts-core`
  (`E:/projects/bohpts/bohpts-core`) MUST host a `BohptsRuntimeStateProvider` class
  implementing `RuntimeStateProvider` directly (no `extends` — there is no vanilla
  `nx-gs-runtime-l2j` to inherit from in MVP), plus a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider` resource pointing
  to it. Provider contract:
  - `schemaName()` = `"bohpts"`
  - `mappings()` returns exactly one `RuntimeEntityMapping<CharacterRuntimeDto>` for
    the `character` entity:
    - `entityName()` = `"character"` (collides with `db-sync` `character` — see R4)
    - `dtoType()` = `CharacterRuntimeDto.class`
    - `snapshot()` iterates `l2e.gameserver.model.GameObjectsStorage.getPlayers()`
      — static accessor on bohpts' canonical online-player storage (backed by
      napile `CHashIntObjectMap<Player>`, returns a live `Collection<Player>`
      view, concurrent-safe for read-only iteration). Filters `player.isOnline()`
      (storage may briefly contain just-logged-out instances). Produces a
      `RuntimeRow` per online `Player` with `pk = player.getObjectId()` and
      `dto` populated from live field accessors. Wraps the iteration in a
      defensive `new ArrayList<>(...)` so HP/MP/CP/coords reads happen on a
      stable iteration set even if the underlying map mutates mid-tick.
    - `hash(dto)` — FNV-1a 64-bit over all non-null hash-relevant fields
      (vitals, vit, coordinates).
  - The package for `BohptsRuntimeStateProvider` inside bohpts-core is
    `l2e.gameserver.l2nx` (same as `BohptsDbSchemaProvider`; both providers
    coexist as siblings).

- [done] R10. `CharacterRuntimeDto` MUST ship in `nx-gs-adapter-api` package
  `app.l2nx.gs.adapter.api.kafka.sync.runtime.character`, parallel to
  `kafka.sync.db.character.CharacterDbDto`. All fields except `id` are nullable per the
  project-wide DTO convention — tenants without a given concept (e.g. no vitality
  mechanic) leave the field unset, Gson omits it on the wire (`serializeNulls=false`).
  Field set:
  - `long id` — `NOT NULL`, primary key (matches `db-sync` `CharacterDbDto.id`,
    same `charId` / `objectId`)
  - `@Nullable Integer curHp`, `@Nullable Integer maxHp`
  - `@Nullable Integer curMp`, `@Nullable Integer maxMp`
  - `@Nullable Integer curCp`, `@Nullable Integer maxCp`
  - `@Nullable Integer curVit`, `@Nullable Integer maxVit` — L2-specific vitality
    stamina; tenants without vitality mechanic leave both null
  - `@Nullable Integer x`, `@Nullable Integer y`, `@Nullable Integer z` —
    world coordinates

  Java 8 POJO, hand-written builder, equals/hashCode/toString. Same shape conventions
  as the existing `CharacterDbDto` in `kafka.sync.db.character`.

- [done] R11. **Module versions:**
  - `nx-gs-adapter-api` = `0.11.0` (breaking: namespaced `ConnectResponse.syncTopics`
    shape + `heartbeatTopic`-at-root per [`adapter-bootstrap`
    R17](001-adapter-bootstrap.md); new `RuntimeStateProvider` /
    `RuntimeEntityMapping` SPI; new `CharacterRuntimeDto`)
  - `nx-gs-adapter-core` bumps to handle the new `ConnectResponse` parse shape (exact
    version TBD when the slice ships; 1.0.0 deferred per project-wide policy)
  - `nx-gs-runtime-sync-core` = `0.1.0` (new artifact)
  - `nx-gs-db-sync-core` and `nx-gs-kafka` carry no contract change in this slice —
    versions stay where they are unless touched independently.

**Should:**

- [done] R12. The runtime engine SHOULD surface per-module health on
  `HeartbeatEvent.enabledModules` via `ModuleStatus`. Stats slot:
  `{name: "runtime-sync", state: ACTIVE/DEGRADED/...,
stats: {entities: List<EntityStats>}}`. Per-entity `EntityStats`:
  `{name, state, rowCount, lastTickEpochMs, lastTickDurationMs, lastTickChanges,
failedAcks, timedOutAcks, consecutiveErrors}`. Same shape as `db-sync` per
  [`cdc-engine` R10](005-cdc-engine/spec.md) — operators see both modules
  side-by-side in heartbeat.
  - SC4. `rowCount` = size of last successful snapshot (number of online entities for
    character); `lastTickChanges` = NEW + CHANGED count (GONE excluded since not
    published). `failedAcks` / `timedOutAcks` surface Kafka tail-health per
    entity — non-zero values flip the entity slot's `state` to `DEGRADED`.

**Could:**

- [todo] R13. The runtime engine COULD support per-entity tick interval override via
  `l2nx.runtime-sync.entities.<name>.tick-interval-seconds`. Comes when a real ops case
  demands it (e.g. coordinates need finer granularity than vitals).

**Non-goals:**

- **Pets / summons / NPCs / mobs** — only player characters in MVP. Boss-runtime is a
  separate slice once a real platform consumer needs it.
- **Buff / debuff state** — separate entity (`character_effects` or similar) in a follow-up
  slice. Not bundled into `CharacterRuntimeDto`.
- **Position smoothing / interpolation** — engine emits raw coordinates from the snapshot;
  client-side interpolation is the platform consumer's concern.
- **Per-field deltas** — every change emits the full DTO. Field-level diff is a
  bandwidth/complexity trade-off not justified at MVP entity counts.
- **Tombstone-on-logout** — see R5. `db-sync` owns "permanently gone" semantics; runtime
  silently drops absent entities from snapshot tracking.
- **Coordinate privacy / scope filtering** — every online character's coordinates are
  surfaced. Privacy-respecting scope (e.g. "only show online status, not coordinates" or
  "hide GMs") is a separate slice; needs an operator-level toggle when it ships.
- **Explicit backpressure** — relies on the existing producer buffer (`buffer.memory`) +
  replay-on-fail (snapshot not advanced for unacked publishes) for absorption. MVP entity
  is bounded at ~10k online characters per server (~2 MB burst at tick boundary, fits
  comfortably in the default 32 MB producer buffer). Explicit rate limiting / tick
  spreading / streaming snapshot iteration ships when a high-cardinality runtime entity
  (e.g. items in inventories of all online players, ~1M+) lands.
- **Event-driven push from host** — the engine pulls. Hooking into in-game events
  (`onHpChange`, `onAutofarmToggle`) would be invasive on the host code-path; pull-based
  snapshot+diff is operator-friendly and keeps the SPI surface small.
- **Cross-module entity coordination** — `db-sync.character` and `runtime-sync.character`
  publish independently to different topics. Platform-side consumers join them by `id` if
  needed. Engine does NOT serialize-order the two streams.
- **Vanilla `nx-gs-runtime-l2j` artifact** — deferred until a second non-bohpts customer
  arrives. Same YAGNI rationale as `nx-gs-db-l2j` per
  [`db-sync` Non-goals](003-db-sync/spec.md).

## Open questions

- [assumed: `RuntimeEntityMapping.snapshot()` returns a defensive copy of the host's live
  collection. Bohpts impl wraps `GameObjectsStorage.getPlayers()` in
  `new ArrayList<>(...)` at iteration entry. The underlying napile `CHashIntObjectMap`
  is concurrent-safe for read-only iteration without copying, but a single tick reads
  HP/MP/CP/coords for ~10k entries and we want a stable iteration target; the copy
  costs ~80 KB / ~100 µs and is negligible against the 10s tick budget.]
- [assumed: Snapshot+diff state is in-memory only — wiped on `onDisconnect` and rebuilt
  on next `onConnect`. Same persistence model as `db-sync`. First tick after reconnect
  publishes the full snapshot as NEW (since prev is empty) — re-syncs every online
  entity to the platform.]
- [resolved: FNV-1a 64-bit hash helper lives in
  `app.l2nx.gs.commons.hash.Fnv1a64` (published `:nx-gs-commons`) — usable by
  both engine and tenant providers. Provider's `mapping.hash(dto)` return value
  is opaque to the engine; algorithm is the provider's choice. FNV-1a is the
  recommended default but not enforced.]
- [assumed: `entityName()` collision between `db-sync.character` and
  `runtime-sync.character` is intentional and resolved by namespaced topics
  (`syncTopics.db.character` vs `syncTopics.runtime.character`). Single-namespace
  collision (two providers in the same module declaring the same entity) is a
  configuration error that the engine logs as ERROR and rejects at startup.]
- [resolved: bohpts-core accessor — `l2e.gameserver.model.GameObjectsStorage.getPlayers()`
  (static), returns `Collection<Player>` view over napile `CHashIntObjectMap`.
  `getPlayer(int objId)` and `getAllPlayersCount()` are companion accessors. Wired
  into `BohptsRuntimeStateProvider.snapshot()` per R9.]
- [resolved: Tick interval default = 10s. Confirmed against ~10k online budget on
  bohpts. Sub-5s freshness for specific entities (e.g. future raid-boss HP) is
  routed through R13 per-entity override when those entities ship.]
- [resolved: `publish-flush-seconds` is **per-module** —
  `l2nx.runtime-sync.publish-flush-seconds` (default 5s) is independent of
  `l2nx.cdc-engine.publish-flush-seconds`. Per R5 SC3. Engines have different tick
  cadences (10s runtime vs 60s cdc) and the flush window should track that, not be
  shared.]
- [resolved: thread-per-entity replaced with a shared bounded pool
  (`ScheduledThreadPoolExecutor`) sized by `l2nx.runtime-sync.workers`
  (default `max(2, min(entities, cores/2))`). Per R5 SC5. Scales to multi-entity
  configs (party, siege, raid-boss, …) without spawning N daemon threads;
  per-entity overlap is guarded by `AtomicBoolean ticking` → skip+WARN on
  overlap.]
- [resolved: `provider.mappings()` is invoked exactly once per `onConnect` and
  cached for the lifetime of the connection (was previously called twice — once
  for the start-up log, once for engine wiring — which surprised providers that
  materialized the list on each call). Cache cleared on `onDisconnect`.]
- [resolved: duplicate `entityName()` values across a single
  `RuntimeStateProvider.mappings()` list are rejected at engine start with the
  module transitioning to `STATE_FAILED`. Distinct from the cross-module
  `db-sync.character` vs `runtime-sync.character` collision which is allowed by
  design (namespaced topics).]

## Links

- Sibling feature (Tier-1 SPI): [`docs/specs/002-adapter-modules/spec.md`](002-adapter-modules/spec.md)
- Sibling feature (topic delivery contract):
  [`docs/specs/001-adapter-bootstrap.md`](001-adapter-bootstrap.md) R17
- Sibling feature (parallel module): [`docs/specs/003-db-sync/spec.md`](003-db-sync/spec.md) —
  same SPI plumbing, different data source (MySQL vs in-memory)
- Sibling feature (algorithm reference):
  [`docs/specs/005-cdc-engine/spec.md`](005-cdc-engine/spec.md) — hash-and-diff snapshot pattern

---

## Technical design

### Overview

Runtime sync is a sibling adapter module to `db-sync`, shipped as the
`nx-gs-runtime-sync-core` artifact. It schedules one tick task per declared
`RuntimeEntityMapping` onto a shared bounded `ScheduledThreadPoolExecutor`
(`nx-runtime-sync-pool-N`, sized by `l2nx.runtime-sync.workers`), ticks at a
configurable interval (default 10s), pulls a `Iterable<RuntimeRow>` snapshot from a
Tier-2 `RuntimeStateProvider` SPI implemented in the host JAR, computes an FNV-1a
64-bit hash per row, diffs against the previous tick's hash map (whose
`defaultReturnValue` is the `MISSING_HASH = Long.MIN_VALUE` sentinel so a legitimate
`0L` hash cannot be confused with absence), and publishes NEW + CHANGED rows as
`SyncEvent<T>` to the per-entity Kafka topic resolved from
`ConnectResponse.syncTopics.runtime[entityName]`. GONE rows are silently dropped (no
tombstone) — `db-sync` owns "permanently gone" semantics. `CycleResult` reports
`DEGRADED` whenever any publish future failed or timed out; `failedAcks` /
`timedOutAcks` counters surface on the per-entity heartbeat slot so Kafka outages
are visible to operators. MVP entity is `character` (vitals + position) against
bohpts.

### Structure

- `nx-gs-runtime-sync-core/` [planned] — module artifact root
- `nx-gs-runtime-sync-core/src/main/java/app/l2nx/gs/runtime/sync/` [planned] —
  package root, parallel to `app.l2nx.gs.db.sync`
- `nx-gs-runtime-sync-core/src/main/java/app/l2nx/gs/runtime/sync/RuntimeSyncModule.java`
  [planned] — Tier-1 `AdapterModule` impl, `name() = "runtime-sync"`
- `nx-gs-runtime-sync-core/src/main/java/app/l2nx/gs/runtime/sync/engine/` [planned] —
  per-entity tick loop, snapshot+diff, publish
- (hash helper) — uses published `app.l2nx.gs.commons.hash.Fnv1a64` from
  `:nx-gs-commons`; no engine-internal copy
- `nx-gs-runtime-sync-core/src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule`
  [planned] — ServiceLoader descriptor
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/RuntimeStateProvider.java`
  [planned] — Tier-2 SPI
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/RuntimeEntityMapping.java`
  [planned] — per-entity mapping contract
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/RuntimeRow.java`
  [planned] — `{ long pk, T dto }` row record
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/sync/runtime/character/CharacterRuntimeDto.java`
  [planned] — wire DTO

Bohpts-side (in `bohpts-core` repo, not this monorepo):

- `bohpts-core/core/src/main/java/l2e/gameserver/l2nx/BohptsRuntimeStateProvider.java`
  [planned] — Tier-2 impl, sibling of `BohptsDbSchemaProvider`
- `bohpts-core/core/src/main/java/l2e/gameserver/l2nx/CharacterRuntimeMapping.java`
  [planned] — `RuntimeEntityMapping<CharacterRuntimeDto>` for the `character` entity
- `bohpts-core/core/src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider`
  [planned] — ServiceLoader descriptor

### Key components

- **RuntimeSyncModule** [done] (implements R1) — `AdapterModule` entry point.
  Resolves `RuntimeStateProvider` via ServiceLoader at `start()`, calls
  `provider.mappings()` exactly once on `onConnect` and caches the result (was
  previously called twice — once for the log, once for wiring), validates the
  topic map and mapping uniqueness, schedules per-entity tick tasks on the shared
  pool, surfaces module health on heartbeat. Cached mappings are cleared on
  `onDisconnect`.
- **Shared scheduler pool** [done] (implements R5 SC5) —
  `ScheduledThreadPoolExecutor` sized by `l2nx.runtime-sync.workers`
  (default `max(2, min(entities, cores/2))`). Threads named
  `nx-runtime-sync-pool-N`, daemon, with a module-wide uncaughtExceptionHandler.
  `setRemoveOnCancelPolicy(true)` so cancelled per-entity tasks are evicted
  immediately. Replaces the legacy thread-per-entity model.
- **EntityTickTask** [done] (implements R5, R8) — `Runnable` scheduled at fixed
  rate per entity. Per-entity `AtomicBoolean ticking` guard — if the previous
  tick is still running when the next fires, the new tick is skipped with a
  WARN (rather than queued, which would exhaust the pool under back-pressure).
  Body: call `mapping.snapshot()` inside `try/catch (Throwable)` (a buggy
  provider degrades the entity for one tick instead of killing the scheduler
  task), hash each row via `mapping.hash(dto)` into a fresh
  `Long2LongOpenHashMap` with `defaultReturnValue(Long.MIN_VALUE)`, diff vs
  previous, publish via `SyncEventPublisher`, walk in-flight futures
  (done-first drain, deadline-bounded wait on pending, respects interrupt),
  swap in only the acked subset of the new snapshot. `CycleResult.degraded`
  is returned on any publish failure / timeout so the heartbeat reflects
  Kafka tail-health.
- **SyncEventPublisher** [done] — wraps `NxKafka` producer. Reuses the same
  `inFlight`-future + flush-at-tick-end mechanism `db-sync` has on a per-window basis;
  runtime-sync has no windows, so the equivalent flush boundary is the tick itself
  (parallel implementation, not literal reuse — module isolation).
- **EntityStatsTracker** [done] — accumulates per-cycle counters
  (`failedAcks`, `timedOutAcks`, `lastTickChanges`, etc.) into the heartbeat
  per-entity slot.
- **Fnv1a64** (implements R6) — `app.l2nx.gs.commons.hash.Fnv1a64` from published
  `:nx-gs-commons` artifact. Public API: `long start()`, `long mix(long state, long
value)` / `mix(state, int)` / `mix(state, boolean)` / `mix(state, CharSequence)`.
  Provider's `mapping.hash(dto)` chains these over fields it cares about. Engine
  never inspects the hash, only compares longs — algorithm is the provider's choice;
  FNV-1a is the recommended default.
- **RuntimeStateProvider SPI** [planned] (implements R2, R3, R4) — Tier-2 SPI on
  `nx-gs-adapter-api`. Mirror shape to `DbSchemaProvider`: `schemaName()` +
  `mappings()`. ServiceLoader descriptor expected at
  `META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider`.
- **RuntimeEntityMapping<T> SPI** [planned] (implements R4) — per-entity contract:
  `entityName()`, `dtoType()`, `snapshot()`, `hash(T)`. Notice the absence of any
  `mapRow(ResultSet)` or DB-schema concepts — runtime SPI is pure-Java, host-internal.
- **CharacterRuntimeDto** [planned] (implements R10) — Java 8 POJO in
  `kafka.sync.runtime.character` package. `id` non-null, all other fields `@Nullable
Integer`. Hand-written builder, equals/hashCode/toString.
- **BohptsRuntimeStateProvider** [planned] (implements R9) — bohpts-side concrete
  provider. Lives in `bohpts-core/l2e.gameserver.l2nx`, sibling of
  `BohptsDbSchemaProvider`. Single mapping for the `character` entity.

### Data flows

End-to-end per tick (one entity, e.g. `character`):

1. Tick task fires (scheduled-at-fixed-rate on the shared pool, every 10s by
   default). If `AtomicBoolean ticking` is already set → skip+WARN and exit.
2. Calls `mapping.snapshot()` inside `try/catch (Throwable)` →
   `Iterable<RuntimeRow<CharacterRuntimeDto>>`
   (bohpts impl: `new ArrayList<>(GameObjectsStorage.getPlayers())`, filtered by
   `player.isOnline()`). On throw → `CycleResult.degraded(elapsed)` + WARN, no
   publishes, entity transitions to `DEGRADED` for this tick.
3. For each `RuntimeRow{pk, dto}`:
   - Compute `hash = mapping.hash(dto)` (FNV-1a 64-bit over vitals + coords)
   - Insert `{pk → hash}` into `currentSnapshot: Long2LongOpenHashMap` whose
     `defaultReturnValue` is `MISSING_HASH = Long.MIN_VALUE`
4. Diff `currentSnapshot` vs `prevSnapshot` (both share the `MISSING_HASH`
   sentinel — absence and a legitimate `0L` hash are now distinguishable):
   - `prev.get(pk) == MISSING_HASH` → emit `SyncEvent.create(pk, dto)`
   - PK in both, hashes differ → emit `SyncEvent.update(pk, dto)`
   - PK in prev, not in current → no-op (drop silently from tracking)
5. For each emit: `SyncEventPublisher.publish(topic, pk, event)` → returns
   `CompletableFuture<Void>`; future enters `inFlight: Map<pk, Future>`
6. Walk in-flight: drain done futures first, then deadline-bounded wait on the
   pending tail up to `publish-flush-seconds`. Respects thread interrupt and
   breaks early on shutdown.
7. For each acked future → write `{pk → hash}` from `currentSnapshot` into the
   "carrier-forward" snapshot for next cycle. Failed/timed-out futures: leave
   previous `prevSnapshot[pk]` (if any) untouched → forces replay next tick,
   increment `failedAcks` / `timedOutAcks` counters.
8. Replace `prevSnapshot = carrierForward`
9. Update heartbeat stats: `lastTickEpochMs`, `lastTickDurationMs`,
   `lastTickChanges = NEW + CHANGED`, `rowCount = currentSnapshot.size()`,
   `failedAcks`, `timedOutAcks`. `CycleResult` is `DEGRADED` if either ack
   counter is non-zero, otherwise `HEALTHY`.

Shutdown: `stop()` cancels scheduled tasks and waits up to
`max(2, publishFlushSeconds + 1)` seconds on `awaitTermination` so in-flight
publishes can drain (the prior fixed 2s lost data once `publishFlushSeconds`
grew past it).

### Data model

In-memory only; no DB tables, no persistence.

- **prevSnapshot** [planned] — `Long2LongMap` per entity. Cleared on `onDisconnect`,
  repopulated on next reconnect's first tick (every entity emits as NEW after
  reconnect — re-syncs the platform's live view).

Wire DTO (Kafka payload):

- **CharacterRuntimeDto** [planned] — `kafka.sync.runtime.character.CharacterRuntimeDto`
  in `nx-gs-adapter-api`. Field set per R10. Wrapped in `SyncEvent<CharacterRuntimeDto>`
  envelope (the same envelope `db-sync` uses; lives at `kafka.sync.db` and is reused).

### Integration points

- **Tier-1 SPI: `AdapterModule`** [planned] — ServiceLoader registration in
  `META-INF/services`. `nx-gs-adapter-core` discovers `RuntimeSyncModule` alongside
  `DbSyncModule`; both modules coexist.
- **Tier-2 SPI: `RuntimeStateProvider`** [planned] — defined in `nx-gs-adapter-api`,
  consumed by `nx-gs-runtime-sync-core` engine. Bohpts-core ships
  `BohptsRuntimeStateProvider` impl + ServiceLoader descriptor.
- **`ConnectContext.syncTopics().runtime()`** [planned] — engine reads its per-entity
  Kafka topic from this map. Map shape and adapter-side parsing are owned by
  [`adapter-bootstrap` R17](001-adapter-bootstrap.md).
- **`HeartbeatEvent.enabledModules`** [planned] — surface
  `{name: "runtime-sync", state, stats: {entities: [...]}}` per R12. Same envelope
  shape as `db-sync` — operators read both modules side by side.
- **bohpts in-memory store** [planned] —
  `l2e.gameserver.model.GameObjectsStorage.getPlayers()` is the read source for the
  `character` entity. Returns a `Collection<Player>` view over a napile
  `CHashIntObjectMap` (concurrent-safe for read-only iteration). Provider wraps in
  `new ArrayList<>(...)` for stable iteration and filters `player.isOnline()` to
  exclude just-logged-out instances still present in the map. Read-only — never
  mutates game state.
- **`NxKafka` producer** — reused via `SyncEventPublisher`. No new Kafka init code; the
  producer instance built at adapter bootstrap (per `adapter-bootstrap` R6) is shared
  across all sync modules.

### Decisions

- **Decision:** Pull-based snapshot+diff per tick, not event-driven push.
  **Why:** Pull keeps the SPI surface small (one `snapshot()` method) and non-invasive
  on the host code-path. Event hooks (`onHpChange`, `onLogin`, `onAutofarmToggle`) would
  require touching dozens of game-server call sites and risk listener-list contention on
  hot threads (regen ticks fire hundreds of times per second across thousands of
  players). The trade-off — sub-tick freshness for high-frequency events — is acceptable
  per the project's existing live-data SLA (10s freshness for vitals).

- **Decision:** Recommended hash algorithm = FNV-1a 64-bit, lifted to published
  `:nx-gs-commons` (`app.l2nx.gs.commons.hash.Fnv1a64`).
  **Why:** `db-sync` uses MySQL-native CRC32 because the hash runs on the DB side; the
  algorithm is forced by the platform. Runtime is pure Java, free choice. Considered:
  CRC32 (`java.util.zip.CRC32`, JDK), CRC32C (Java 9+, ruled out by Java 8 baseline),
  xxHash64 / MurmurHash3 (added dep, overkill). FNV-1a 64-bit is the sweet spot:
  ~15 LOC, zero deps, mul+xor (faster than CRC32 table lookup), 64-bit collision safety
  (~0% on 10k entries vs ~1% birthday collision for 32-bit on the same set), structural
  hashing of typed fields skips intermediate string serialization. Engine treats the
  returned long as opaque — provider can swap the algorithm without engine changes.

- **Decision:** No tombstone on logout / disconnect.
  **Why:** `db-sync` owns "permanently gone" (clan disbanded, character deleted).
  Runtime "absence" is transient — the same character will be back on next login. Platform
  consumers treat the last-published runtime state as "stale snapshot" by their own
  freshness logic (e.g. cross-reference with heartbeat liveness, or apply a TTL on
  read). Emitting tombstones every logout would double the runtime topic's event rate
  and require subscribers to distinguish "char logged out" from "char actually deleted"
  — they get the latter from `db-sync` already.

- **Decision:** `entityName` collision between modules is allowed; namespace separation
  is in `syncTopics`.
  **Why:** Operator-friendly mental model: `character` is a domain concept, present in
  both DB and runtime data sources. Forcing `character` (db) vs `character_runtime` (runtime)
  in the entity name space leaks implementation source into domain vocabulary. The
  ConnectResponse refactor (`syncTopics: { db, runtime, dp }`) provides clean
  resolution at topic-lookup time without polluting entity names.

- **Decision:** Per-module `publish-flush-seconds` —
  `l2nx.runtime-sync.publish-flush-seconds` (default 5s) is independent of
  `l2nx.cdc-engine.publish-flush-seconds`.
  **Why:** Tick cadences differ (10s for runtime vs 60s for cdc-engine) and the flush
  window should be sized against the corresponding tick budget. Sharing one knob would
  force operators to compromise — set it long enough for cdc and runtime tail latency
  bites; set it short and cdc starts replaying frequently. Separate keys cost nothing
  in code (each engine reads its own property) and let each module tune independently.

- **Decision:** Snapshot state is in-memory only, wiped on `onDisconnect`, rebuilt on
  reconnect.
  **Why:** Same persistence model as `db-sync`. Persisting snapshot to disk would
  require a write path on disconnect (operator-hostile during shutdown) and a recovery
  path on next start. Reconnect-and-re-sync is acceptable: first tick after reconnect
  publishes everything as NEW, ~10k events on the wire, finishes in seconds.

- **Decision:** Shared bounded thread pool (`ScheduledThreadPoolExecutor`) over
  thread-per-entity.
  **Why:** As more runtime entities ship (party, siege, raid-boss, …) the
  thread-per-entity model wastes daemon threads on cheap entities and concentrates
  scheduling logic per loop. The shared pool sized by `l2nx.runtime-sync.workers`
  (default `max(2, min(entities, cores/2))`) scales naturally, plays well with
  `setRemoveOnCancelPolicy(true)` for clean teardown, and the per-entity
  `AtomicBoolean ticking` guard turns overlapping ticks into observable
  skip-with-WARN rather than silent queue growth.

- **Decision:** `MISSING_HASH = Long.MIN_VALUE` sentinel for the hash map's
  `defaultReturnValue`.
  **Why:** `Long2LongOpenHashMap.get(absentKey)` returns `0L` by default —
  collides with a legitimate `0L` hash from a provider. `Long.MIN_VALUE` is
  outside the practical FNV-1a output range and gives a single, unambiguous
  test for absence (`prev.get(pk) == MISSING_HASH` → NEW).

- **Decision:** `provider.mappings()` is invoked once per `onConnect` and
  cached.
  **Why:** The earlier code called it twice — once for the start-up log,
  once for engine wiring — which surprised providers that materialized the
  list eagerly on each call (e.g. read live game state). Single call +
  cache keeps the contract simple and predictable.

- **Decision:** Uniqueness validation on `entityName()` at engine start.
  **Why:** A provider declaring two mappings with the same `entityName`
  would clobber each other's `prevSnapshot` / topic mapping silently. Fail
  loudly at startup (module → `STATE_FAILED`) so the operator fixes the
  classpath rather than chasing missing data.

### Extension points

- **Add a new runtime entity** — declare an additional `RuntimeEntityMapping<NewDto>`
  in the host's `RuntimeStateProvider.mappings()` list. Engine spins up an additional
  daemon thread automatically. Platform side must add the corresponding entry to
  `ConnectResponse.syncTopics.runtime[newEntityName]`.

- **Add a new tenant** — implement `RuntimeStateProvider` in the tenant's host JAR with
  a `META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider` descriptor.
  Same plug-in shape as `DbSchemaProvider`. No artifact published to Maven Central; the
  provider lives in the tenant's private repo.

- **Per-entity tick rate override (Could R13)** — engine reads
  `l2nx.runtime-sync.entities.<name>.tick-interval-seconds` first, falls back to global
  `l2nx.runtime-sync.tick-interval-seconds`, then to default (10s).

- **Explicit backpressure (deferred)** — when a high-cardinality runtime entity ships,
  add per-entity rate limiter at the publisher boundary (records/sec cap with sleep
  back-off) and/or convert snapshot iteration to a streaming pattern (don't buffer the
  full 1M-row snapshot; iterate-and-publish with periodic flush). Engine's current
  contract (single-shot `snapshot()` call) accommodates streaming via a lazy
  `Iterable` impl on the provider side without SPI breakage.
