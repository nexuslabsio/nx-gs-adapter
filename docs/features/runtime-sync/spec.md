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
> - Tier-1 SPI (`AdapterModule` + ServiceLoader wire-up) lives in
    > [`adapter-modules`](../adapter-modules/spec.md). `runtime-sync` is one more module on the
    > same plane as `db-sync`.
> - Namespaced `syncTopics` (`db` / `runtime` / `dp`) and `heartbeatTopic`-at-root reorg lives
    > in [`adapter-bootstrap` R17](../adapter-bootstrap/spec.md). The runtime engine reads its
    > topic from `ctx.syncTopics().runtime()` keyed by `entityName()`.
> - The hash-and-diff algorithm shape borrows from
    > [`cdc-engine`](../cdc-engine/spec.md) (per-PK hash, snapshot swap on Kafka ack), but
    > the implementation is independent: no DB phases, no `BIT_XOR(CRC32(...))` SQL, no
    > sliding-window scans. Snapshot source is `Iterable<RuntimeRow>` from the SPI; hash is
    > FNV-1a 64-bit computed in Java over typed fields.
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
       [`cdc-engine`](../cdc-engine/spec.md). `failedAcks` / `timedOutAcks` counters
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
  [`adapter-bootstrap` R17](../adapter-bootstrap/spec.md). Per-entity behavior:
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
  `kafka.sync.db.character.CharacterDto`. All fields except `id` are nullable per the
  project-wide DTO convention — tenants without a given concept (e.g. no vitality
  mechanic) leave the field unset, Gson omits it on the wire (`serializeNulls=false`).
  Field set:
    - `long id` — `NOT NULL`, primary key (matches `db-sync` `CharacterDto.id`,
      same `charId` / `objectId`)
    - `@Nullable Integer curHp`, `@Nullable Integer maxHp`
    - `@Nullable Integer curMp`, `@Nullable Integer maxMp`
    - `@Nullable Integer curCp`, `@Nullable Integer maxCp`
    - `@Nullable Integer curVit`, `@Nullable Integer maxVit` — L2-specific vitality
      stamina; tenants without vitality mechanic leave both null
    - `@Nullable Integer x`, `@Nullable Integer y`, `@Nullable Integer z` —
      world coordinates

  Java 8 POJO, hand-written builder, equals/hashCode/toString. Same shape conventions
  as the existing `CharacterDto` in `kafka.sync.db.character`.

- [done] R11. **Module versions:**
    - `nx-gs-adapter-api` = `0.11.0` (breaking: namespaced `ConnectResponse.syncTopics`
      shape + `heartbeatTopic`-at-root per [`adapter-bootstrap`
      R17](../adapter-bootstrap/spec.md); new `RuntimeStateProvider` /
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
  [`cdc-engine` R10](../cdc-engine/spec.md) — operators see both modules
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
  [`db-sync` Non-goals](../db-sync/spec.md).

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

- Sibling feature (Tier-1 SPI): [`docs/features/adapter-modules/spec.md`](../adapter-modules/spec.md)
- Sibling feature (topic delivery contract):
  [`docs/features/adapter-bootstrap/spec.md`](../adapter-bootstrap/spec.md) R17
- Sibling feature (parallel module): [`docs/features/db-sync/spec.md`](../db-sync/spec.md) —
  same SPI plumbing, different data source (MySQL vs in-memory)
- Sibling feature (algorithm reference):
  [`docs/features/cdc-engine/spec.md`](../cdc-engine/spec.md) — hash-and-diff snapshot pattern
