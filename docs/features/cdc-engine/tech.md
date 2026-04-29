# CDC Engine — tech

> Covers: spec.md
> Sibling: [`db-sync/tech.md`](../db-sync/tech.md) — module wiring + Tier-2 SPI shape

## Overview

The engine lives inside `:nx-gs-db-sync-core` under `app.l2nx.gs.db.sync.engine`. It is
wired by `DbSyncModule` once `provider.mappings()` is available (Phase 2): one
`EntitySyncTask` instance per `EntityMapping`, each running on its own daemon
`ScheduledExecutorService`. The task implements the CRC32 two-phase protocol on every
tick — always in PK-windowed mode — walking N windows back-to-back inside a single
tick (where N is derived from `rowsPerWindow` config and the entity's PK range). It
holds a fastutil `Long2IntOpenHashMap` snapshot per entity and publishes `SyncEvent`s
via `NxKafka` to the topic delivered by `ConnectResponse.syncTopics` for that entity.
Per-entity operational state is kept as a `volatile`-published `EntityStats` snapshot
read by `HeartbeatService` per heartbeat tick.

## Structure

- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/`
    - `CdcEngine.java` [planned] — orchestrator: holds the `ScheduledExecutorService`,
      owns one `EntitySyncTask` per `EntityMapping`, exposes `currentEntityStats()` for
      heartbeat enrichment (R10)
    - `EngineConfig.java` [planned] — value bag holding the resolved per-engine and
      per-mapping parameters (R15); built by `CdcEngine` constructor from
      `ConfigResolver` + `provider.mappings()`; immutable thereafter
    - `ConfigResolutionLogger.java` [planned] — emits the structured startup log
      block (R16); single static helper called once at `CdcEngine.start()`
    - `EntitySyncTask.java` [planned] — per-entity `Runnable` invoked by the
      scheduler; on each tick walks every PK window back-to-back, running the
      Phase 1 → diff → Phase 2 → publish → window-swap protocol per window (R1, R2)
    - `TopicResolver.java` [planned] — SAM `String resolveTopic(String entityName)`
      backed by `ConnectResponse.syncTopics`; injected at engine construction; used
      by `SyncEventPublisher` (R17)
    - `Phase1Hasher.java` [planned] — issues the CRC32 query, reads PK as `long`,
      returns `Long2IntOpenHashMap` (R1, R11)
    - `Phase2Fetcher.java` [planned] — given `LongSet changedPks`, builds chunked
      `IN (...)` queries, calls `mapping.mapRow(rs)`, returns mapped DTOs (R1, R11)
    - `SnapshotStore.java` [planned] — per-entity `Long2IntOpenHashMap` holder;
      thread-confined to its `EntitySyncTask` thread (R4); `swapWindow(from, to,
      windowMap)` updates only the `[from, to]` PK range
    - `ChangeSet.java` [planned] — value bag `{ LongSet created, LongSet updated,
      LongSet deleted }` returned by diff (R1)
    - `WindowPlanner.java` [planned] — runs `MIN(pk), MAX(pk)` recompute at start of
      cycle and computes `windowCount = max(1, ceil(pkRange / rowsPerWindow))`;
      returns ordered list of `[fromPk, toPk]` boundaries (R2)
    - `EntityStatsTracker.java` [planned] — assembles `EntityStats` snapshots;
      published via a `volatile List<EntityStats>` for cross-thread heartbeat reads
      (R10)
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/kafka/`
    - `SyncEventPublisher.java` [planned] — translates `(mapping, op, pk, dto|null)` to
      `SyncEvent`, builds Kafka key, calls `NxKafka.instance().send(...)` (R12, R13)
- `nx-gs-db-sync-core/build.gradle.kts` [planned] — adds `implementation
  'it.unimi.dsi:fastutil-core:8.5.x'` (primitive maps only — full `fastutil` not pulled)

## Key components

- **`CdcEngine`** [planned] (R5, R6, R10, R15, R16) — orchestrator owned by
  `DbSyncModule`. Constructor receives `provider.mappings()` + `JdbcConnectionSource` +
  Kafka producer + `ConfigResolver`. Builds an immutable `EngineConfig` once (resolves
  the R15 chain for every mapping). Holds a `Map<EntityMapping<?>, EntitySyncTask>`
  (provider-list order preserved via `LinkedHashMap`) and a single
  `ScheduledExecutorService` with one thread per mapping. `start()` emits the R16
  startup log via `ConfigResolutionLogger`, then schedules each `EntitySyncTask` with
  first-tick delay 0 using the resolved tick interval per mapping. `stop()` cancels
  schedulers and drains in-flight ticks. Exposes `List<EntityStats> currentEntityStats()`
  for heartbeat enrichment — reads from a single `volatile` reference rebuilt at the
  end of every cycle by each task.
- **`EngineConfig`** [planned] (R15) — immutable value bag carrying ONLY engine-
  level globals from `l2nx.properties`:
  ```java
  Duration tickInterval;        // l2nx.cdc-engine.tick-interval-seconds | default 60s
  int rowsPerWindow;            // l2nx.cdc-engine.rows-per-window       | default 500_000
  Duration queryTimeout;        // l2nx.cdc-engine.query-timeout-seconds | default 10s
  Duration publishFlush;        // l2nx.cdc-engine.publish-flush-seconds | default 5s
  EnumMap<EngineParam, SourceTag> sources;   // OPERATOR_OVERRIDE | DEFAULT per param,
                                             // for ConfigResolutionLogger
  ```
  No per-entity entries — every entity uses these same global values. Built by
  `CdcEngine` constructor from `ConfigResolver`; passed by reference to every
  `EntitySyncTask`.

- **Entity → topic map** [planned] (R17) — separate immutable
  `Map<String, String> entityTopics` snapshot taken from
  `ConnectResponse.syncTopics` at engine construction (entries the platform
  delivered). Built alongside `EngineConfig` but tracked separately because its
  source is the connect-response, not l2nx.properties. An entity whose
  `entityTopics.get(entityName)` returns `null` is recorded as `null` and
  permanently `DEGRADED` per R17.
- **`ConfigResolutionLogger`** [planned] (R16) — static helper invoked once at
  `CdcEngine.start()`. Reads `EngineConfig`, formats the structured log block, emits
  via `NxLog` at `INFO` level on logger `app.l2nx.gs.db.sync.engine.CdcEngine`. Single
  call site; no state.
- **`EntitySyncTask`** [planned] (R1, R2, R5, R7, R9) — per-entity `Runnable`. On
  each tick:
    1. `WindowPlanner.recomputeBoundaries(mapping, rowsPerWindow)` — runs
       `SELECT MIN(<pk>), MAX(<pk>) FROM <tableName>`, computes `windowCount =
       max(1, ceil(pkRange / rowsPerWindow))`, returns ordered list of
       `[fromPk, toPk]` boundaries
    2. For each window in order: `Phase1Hasher.hash` (bounded by `[from, to]`) →
       `SnapshotStore.diff` for that PK range → if non-empty change set →
       `Phase2Fetcher.fetch` for `created ∪ updated` → `SyncEventPublisher.publish`
       per row + per deleted PK → `SnapshotStore.swapWindow(from, to, currentWindow)`
    3. After all windows: build a new `EntityStats`, atomically replace the
       published snapshot
       Wrapped in `SafeRunnable` so any throwable is caught, the entity state transitions
       to `DEGRADED`, and the schedule continues. Small entities (rowCount ≤
       rowsPerWindow) yield exactly 1 window — no separate code path.
- **`Phase1Hasher`** [planned] (R1, R11) — opens
  `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY`. Always issues a windowed
  query: `SELECT <pk>, CRC32(CONCAT_WS(',', <hashedColumns>)) FROM <tableName>
  WHERE <pk> BETWEEN ? AND ?`. Reads PK via `rs.getLong(1)` and CRC32 via
  `rs.getInt(2)` (CRC32 fits in unsigned 32-bit; signed `int` carries the same bytes,
  comparison is bit-exact). Result: `Long2IntOpenHashMap`. Calls
  `Statement.setQueryTimeout` from `l2nx.cdc-engine.query-timeout-seconds`. Closes
  resultset / statement / connection in try-with-resources.
- **`Phase2Fetcher`** [planned] (R1, R11) — given a `LongSet`, builds `SELECT
  <fetchColumns> FROM <tableName> WHERE <pk> IN (?, ?, ...)` chunked at 1000 PKs.
  Per-chunk transaction = `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY`.
  PKs bound via `setLong(...)`. Calls `mapping.mapRow(rs)` per row.
- **`SnapshotStore`** [planned] (R4) — wraps a `Long2IntOpenHashMap` per
  `EntityMapping`. Thread-confined to the task thread (no synchronization needed).
  Provides `diffWindow(from, to, currentWindow) → ChangeSet` (evaluates only PKs in
  the `[from, to]` range), `swapWindow(from, to, currentWindow)` (replaces only the
  window's PK range, keeps entries outside `[from, to]` untouched). Wiped on
  `DbSyncModule.onDisconnect`. NO RAM cap enforcement — operator sizes the host JVM
  to fit the configured entities.
- **`ChangeSet`** [planned] (R1) — value bag with three `LongSet` (fastutil
  `LongOpenHashSet`). Computed in one pass over the previous-window PKs ∪ current-
  window PKs.
- **`WindowPlanner`** [planned] (R2) — runs
  `SELECT MIN(<pk>), MAX(<pk>) FROM <tableName>` at the start of each cycle
  (recompute, NOT cached at connect — auto-extends to capture inserts above the
  prior MAX). Computes `windowCount = max(1, ceil((MAX - MIN + 1) / rowsPerWindow))`,
  divides `[MIN, MAX]` into evenly-sized half-open intervals, returns
  `List<long[]>` of `{from, to}` pairs.
- **`TopicResolver`** [planned] (R17) — SAM `String resolveTopic(String entityName)`
  backed by an immutable `Map<String, String>` snapshot taken from
  `ConnectResponse.syncTopics` at engine construction. Returns `null` for entities
  the platform did not deliver a topic for. Engine resolves once per entity at
  start; result stored in `ResolvedEntityConfig.topic`. Null-topic entities are
  permanently `DEGRADED`.
- **`EntityStatsTracker`** [planned] (R10) — assembles an `EntityStats` per cycle:
  `name = mapping.entityName()` (the domain identifier — `"clan"`, NOT the source
  table `"clan_data"`), `state` (`HEALTHY` / `DEGRADED`), `rowCount` (cumulative
  Phase 1 size across all windows of the cycle), `lastSyncEpochMs`
  (`Instant.now().toEpochMilli()` at successful end of cycle),
  `lastCycleDurationMs` (`System.nanoTime` delta), `lastCycleChanges`
  (`ChangesSummary` from change-set sizes summed across windows),
  `consecutiveErrors` (counter reset on HEALTHY). Published into a
  `volatile List<EntityStats>` field on `CdcEngine` — heartbeat thread reads it via
  `currentEntityStats()`.
- **`SyncEventPublisher`** [planned] (R12, R13) — owns `SyncEvent` envelope
  construction. Kafka key is the row's `long` PK (8 bytes via `LongSerializer`);
  topic is resolved once per entity at engine start via `TopicResolver` (delivered
  via `ConnectResponse.syncTopics`) and stored in the entity's `ResolvedEntityConfig`;
  value is Gson-serialized `SyncEvent { entityName, pk: long, op, payload, timestamp }`.
  Async send via `NxKafka.send` — does not block. Tracks per-cycle
  `CompletableFuture`s in a list; at end of cycle waits with
  `l2nx.cdc-engine.publish-flush-seconds` (default 5s) for all sends to ack. Any
  failure short-circuits the snapshot swap (R13).

## Data flows

### 0. Engine startup — config resolution + log

```
CdcEngine constructor (called by DbSyncModule.start, Phase 2)
  → EngineConfig cfg = resolveEngineConfig(configResolver)
       → cfg.tickInterval   = configResolver.optDuration("l2nx.cdc-engine.tick-interval-seconds")
                                  .orElse(Duration.ofSeconds(60))
       → cfg.rowsPerWindow  = configResolver.optInt("l2nx.cdc-engine.rows-per-window")
                                  .orElse(500_000)
       → cfg.queryTimeout   = configResolver.optDuration("l2nx.cdc-engine.query-timeout-seconds")
                                  .orElse(Duration.ofSeconds(10))
       → cfg.publishFlush   = configResolver.optDuration("l2nx.cdc-engine.publish-flush-seconds")
                                  .orElse(Duration.ofSeconds(5))
       → cfg.sources tagged per param (OPERATOR_OVERRIDE if present in props, else DEFAULT)
  → entityTopics = unmodifiableCopy(ctx.syncTopics())   -- snapshot from connect-response

CdcEngine.start()
  → ConfigResolutionLogger.log(cfg, provider.mappings(), entityTopics)
                                                -- R16, single contiguous INFO block
  → for each mapping in provider.mappings() (linked, order preserved):
        String topic = entityTopics.get(mapping.entityName())
        if (topic == null) {
            stats.markDegraded(mapping.entityName(), "no topic in connect response")
            continue                            -- R17: permanent DEGRADED, no schedule
        }
        scheduler.scheduleWithFixedDelay(taskFor(mapping, topic), 0, cfg.tickInterval, ...)
```

Sample log output (R16):

```
INFO  CdcEngine config: tickInterval=60s [default], rowsPerWindow=500000 [operator-override l2nx.cdc-engine.rows-per-window=500000], queryTimeout=10s [default], publishFlush=5s [default]
INFO  CdcEngine [clan] → topic=bohpts.gs.sync.clans
INFO  CdcEngine [character] → topic=bohpts.gs.sync.characters
INFO  CdcEngine [item] → topic=bohpts.gs.sync.items
```

If platform delivered no topic for an entity:

```
INFO  CdcEngine [audit] → topic=<missing — entity DEGRADED>
```

### 1. Cycle (single windowed strategy)

```
EntitySyncTask.run()  [scheduler thread, daemon]
  → cycleStart = System.nanoTime()
  → try {
      windows = WindowPlanner.recomputeBoundaries(mapping, cfg.rowsPerWindow)
                                                    -- MIN/MAX recompute, evenly partitioned
      totalCreated = totalUpdated = totalDeleted = 0
      for (long[] window : windows) {
          Long2IntOpenHashMap current = Phase1Hasher.hash(mapping, window)
          ChangeSet diff = SnapshotStore.diffWindow(mapping, window, current)
          if (!diff.empty()) {
              List<RowDto> fetched = Phase2Fetcher.fetch(mapping, diff.created ∪ diff.updated)
              List<CompletableFuture<?>> sends = []
              for row in fetched:
                  sends.add(SyncEventPublisher.publish(mapping, op, pk, row))
              for pk in diff.deleted:
                  sends.add(SyncEventPublisher.publish(mapping, DELETED, pk, null))
              CompletableFuture.allOf(sends).get(cfg.publishFlush.getSeconds(), SECONDS)
          }
          SnapshotStore.swapWindow(mapping, window, current)
                                                    -- entries outside [from,to] untouched
          totalCreated += diff.created.size()
          totalUpdated += diff.updated.size()
          totalDeleted += diff.deleted.size()
      }
      stats.markHealthy(totalCreated, totalUpdated, totalDeleted)
    } catch (Throwable t) {
      stats.markDegraded(t)                         -- snapshot NOT advanced for the
                                                    -- failing window
    } finally {
      stats.lastCycleDurationMs = (System.nanoTime() - cycleStart) / 1_000_000
      engine.publishStatsSnapshot()                 -- atomic List<EntityStats> rebuild
    }
```

Small entities (rowCount ≤ rowsPerWindow): `windows` returns a single
`[MIN, MAX]` interval — the loop runs once, operationally identical to a "full
scan".

### 2. Heartbeat enrichment

```
HeartbeatService tick   [heartbeat thread]
  → ModuleRegistry.currentStatuses()
       → DbSyncModule.currentStatus()
            → ModuleStatus.builder()
                .name("db-sync")
                .state(stateFromEntityStates())  -- ACTIVE if all HEALTHY,
                                                 -- DEGRADED if any non-HEALTHY
                .stats(Stats.builder()
                    .pool(jdbcSource.stats().orElse(null))
                    .entities(cdcEngine.currentEntityStats())   -- volatile read
                    .build())
                .build()
  → HeartbeatEvent.enabledModules.add(...)
```

## Decisions

- **CRC32 sufficient — collision risk is per-change, not birthday.** The relevant
  collision probability is "given a row changed, what's the chance its new CRC32 equals
  its old CRC32?" — exactly `1/2^32` per change-event, NOT a birthday-paradox cross-row
  probability. For 12M rows with hundreds of changes per cycle the missed-change
  probability is ~0 per cycle. Alternatives (CRC64 / xxHash) shrink that to `1/2^64` at
  the cost of doubled hashmap RAM (`Long2LongOpenHashMap` instead of
  `Long2IntOpenHashMap`); not justified for game-data eventual-consistency semantics.
- **fastutil for in-memory snapshot.** `Long2IntOpenHashMap` holds 12M entries in
  ~240 MB at default load factor 0.75. Equivalent `HashMap<Long, Long>` would cost
  ~600 MB due to Long box (~48 bytes/entry) + entry chaining. RAM matters for the host
  JVM; the hash domain (32-bit) matches CRC32's natural width. `fastutil-core` (~3 MB)
  keeps only the primitive maps we use; we deliberately don't pull the full `fastutil`
  JAR (~21 MB).
- **`long` PK end-to-end — engine, wire, Kafka key.** Engine internals (fastutil maps,
  JDBC `setLong`/`getLong`), wire payload (`SyncEvent.pk: long`, JSON number), and
  Kafka key (binary `LongSerializer`, 8 bytes) all carry the raw `long`. Platform
  stores the PK as `long` and reads it back as `long` — no client-side
  stringification, no boxing on the wire. Earlier "All IDs as String for cross-schema
  invariance" stance is reversed: cross-schema (UUID / composite) was the rationale
  for stringification, but those schema shapes are explicit Non-goals in MVP. When
  they arrive, a parallel String-PK / composite-PK wire variant is added — not a
  premature stringification of the long-PK happy path.
- **Per-query consistent snapshot, NOT per-cycle.** Each Phase 1 query and each
  Phase 2 chunk runs in its own `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ
  ONLY`. Wrapping the whole entity cycle (N×Phase 1 windows + N×Phase 2 chunks) in
  one REPEATABLE READ would hold a multi-second-to-multi-minute transaction on the
  host DB, growing InnoDB's undo log and slowing the game core's writes — the
  explicit Non-goal in `db-sync` (eventual consistency wins). Per-query snapshot
  keeps each individual scan internally consistent (no torn rows mid-Phase-1)
  without the long-tx penalty.
- **MIN/MAX recompute at start of every cycle, NOT cached at connect.** Cached
  boundaries miss rows inserted above the prior MAX — for an auto-increment PK like
  `items.id` this is a constant correctness bug (every game-server tick produces new
  `items` rows). The recompute query (`SELECT MIN(pk), MAX(pk) FROM tbl`) uses the PK
  index → O(log n) two index lookups, < 50ms even on 12M rows. The cost is negligible
  compared to the Phase 1 scan (20–40s).
- **One scheduler thread per `EntityMapping`.** Thread-confined `SnapshotStore`
  removes the need for any synchronization on the per-entity state. N daemon
  threads for N entities; with ~5 entities per provider this is negligible. A
  shared-pool alternative would force defensive locking on every snapshot access
  path.
- **Sequential cycle order from provider list.** Engine launches one task per
  mapping in the order returned by `provider.mappings()`. Each task runs
  independently on its own thread; entities on different threads can overlap.
  Provider authors arrange the list manually if cross-entity cycle ordering matters
  (informational guideline). No engine-side sort by row count — predictability over
  self-tuning.
- **Single windowed strategy, no `FULL_SCAN`/`SLIDING_WINDOW` enum.** Engine ships
  one scan algorithm: PK-range windowed sweep with `rows-per-window` partition.
  Small entities (rowCount ≤ rowsPerWindow) collapse to a 1-window cycle —
  operationally identical to a "full scan" with no separate code path.
  `SyncStrategy` enum and `EntityMapping.strategy()`/`windowCount()` SPI fields are
  out-of-scope. Earlier proposed dual-strategy design added branching for an
  imagined small-entity speedup that doesn't materialize: a 1-window scan IS the
  full-scan path. Removing the enum simplifies the SPI, the engine code, and the
  config surface (no `window-count` knob).
- **Global query timeout, not per-entity.** `l2nx.cdc-engine.query-timeout-seconds`
  (default 10s) applies to every Phase 1 / Phase 2 query. Per-entity tunability is
  YAGNI for MVP — the timeout's purpose is "don't hang on a stalled query", which
  is uniform across entities. Add `mapping.queryTimeout()` later if a real workload
  pattern demands it.
- **Per-entity stats over heartbeat, not separate Kafka stats topic.** Per-entity
  state (rowCount, lastSyncEpochMs, etc.) is surfaced via
  `HeartbeatEvent.enabledModules[*].stats.entities[]` (R10). Per-entity side-channel
  topics would duplicate the same data, multiply ACL/retention surface, and add
  noise. Heartbeat is the single channel.
- **No engine-side RAM cap.** Earlier draft enforced a per-entity row-count cap and
  marked the entity `SKIPPED` on overflow. Removed: operators size the host JVM to
  fit the configured entities; an entity that grows beyond expected size simply uses
  more RAM. The `EntityState.SKIPPED` constant was removed together with this
  decision (`HEALTHY | DEGRADED` only).
- **No inbound Kafka consumer for dynamic config in MVP.** Adding a consumer for
  `nexus.adapter.sync-config` shifts the adapter from "fully outbound" to "consumer
  thread + reload semantics + race handling between config update and active cycle".
  Significant architectural surface for an unproven need. Static
  `mapping.tickInterval()` requires adapter restart to change cadence — acceptable for
  MVP.
- **Topic creation by the platform, not the adapter.** Adapter publishes; platform
  provisions topics with the right `cleanup.policy=compact` + retention. Avoids
  needing `AdminClient` permissions on the adapter and keeps the adapter's Kafka
  surface to producer-only. `cdc-engine` spec defines wire shape (key + tombstone
  semantics); the platform honours it via topic config.
- **All-or-nothing snapshot swap per cycle.** If any Kafka send fails during the cycle,
  the snapshot is NOT advanced — the same diff is replayed on the next tick. Per-row
  swap (advance after each successful send) gives finer recovery but requires
  bookkeeping per-row swap state. Default to all-or-nothing; flip later if needed.
- **`enable.idempotence=true` Kafka producer.** Cheap, exactly-once-per-session
  guarantee inside Kafka. Already default in modern kafka-clients; engine does not
  toggle it explicitly — relies on `nx-gs-kafka` to keep this default.
- **Engine config from `l2nx.properties` only — no provider-side declarations,
  no per-entity overrides.** All operator-tunable parameters
  (`tick-interval-seconds`, `rows-per-window`, `query-timeout-seconds`,
  `publish-flush-seconds`) live under `l2nx.cdc-engine.*` (matching the
  `adapter-bootstrap` convention `l2nx.platform.*`, `l2nx.kafka.*`,
  `l2nx.heartbeat.*`). Schema providers (`EntityMapping`) declare ONLY
  schema-shape information — entity name, source table, hashed columns, row
  mapper, DTO type. Cadence and window size are operational concerns and live
  with the operator, not the schema. Rationale:
  (1) **Provider/operator separation of concerns** — schema providers describe
  entities; operators tune the engine. Mixing both into the SPI tied behavioral
  changes to provider releases unnecessarily.
  (2) **Single global knob covers MVP** — operator turning all-entities
  faster/slower for diagnostics or load tuning is the only operational gesture
  observed in MVP scenarios; per-entity granularity adds API surface for an
  unproven need.
  (3) **Per-entity granularity arrives via R14's dynamic Kafka config** when a
  real ops case demands it — not via static `l2nx.properties` per-entity keys
  and not via per-mapping SPI methods.
- **Resolution chain logged in plaintext at startup, not just stored.** R16's
  source-tagged log block (`[operator-override | provider | default | connect-response]`)
  is the audit trail — operators reading the adapter's startup output see exactly
  which value won for every parameter without grepping config files vs
  schema-provider source vs platform handshake. The cost is one INFO block per
  engine start, emitted on `app.l2nx.gs.db.sync.engine.CdcEngine` at engine init.
- **Per-entity Kafka topics delivered by platform, not assembled by adapter.** Topic
  names arrive in `ConnectResponse.syncTopics` keyed by `entityName`; the engine
  consumes them via `TopicResolver` injected at construction. Earlier draft had the
  engine assembling `{tenant}.gs.sync.{topicSuffix}` from `mapping.topicSuffix() +
  ctx.tenantSlug`, which forced the schema provider to know the tenant's topic
  naming convention and forced any future renaming through a coordinated provider
  release. Platform-supplied topics decouple naming from the schema provider:
  topic naming policy lives entirely on the platform side; provider authors only
  declare entity identity (`entityName`). An entity without a platform-delivered
  topic is permanently `DEGRADED` per R17 — defensive path for ConnectResponse
  payloads that lose entries during platform-side rollouts; not expected in
  steady-state operation.
- **Persisted previous-snapshot cache (post-MVP, R18) — feasibility analysis.**
  Goal: a host-JVM restart should not trigger a full initial-sync replay (R7) for
  every entity. The in-memory `Long2IntOpenHashMap` per entity could be
  periodically dumped to disk (e.g. `<host>/.nx-adapter/cdc/<tenantSlug>/<serverSlug>/<entityName>.bin`)
  and reloaded on next startup. Feasibility — yes, but with non-trivial constraints
  worth documenting before the dedicated slice:
    1. **Schema invalidation.** The cache must invalidate on any change to
       `hashedColumns` (CRC32 input set) or to `dtoType` / `mapRow` (Phase 2
       output shape). Mechanism: store a `schemaFingerprint` in the cache header
       — hash of `(entityName, tableName, hashedColumns, hashedColumns ordering)`.
       Mismatch on load → discard cache, full resync. Schema-provider version
       bump (e.g. adding a hashedColumn) invalidates correctly without operator
       action.
    2. **Format.** Naive: serialized `Long2IntOpenHashMap` via Java
       Serialization. Better: simple binary format — header (magic, version,
       schemaFingerprint, entryCount) + dense `(long pk, int crc32)` pairs. ~12
       bytes/entry on disk → ~144 MB for 12M items. Survives JVM Java-version
       changes and avoids `serialVersionUID` brittleness.
    3. **fsync cadence.** Every cycle is wasteful (12M-entry rewrite × per-tick
       cadence). Options: dump on shutdown only (loses recent diffs on JVM
       crash), or periodic dump every N cycles (configurable). Preferred:
       shutdown-only via JVM shutdown hook — the worst case (crash) replays
       diffs since last clean shutdown, which is bounded by reboot cadence
       (rare on production game servers). Periodic dump can be added later if
       actual recovery scenarios demand it.
    4. **Filesystem location.** Operator-controlled via
       `l2nx.cdc-engine.cache-dir` config; default `<host-tmp>/nx-adapter/cdc/`.
       Container ephemeral filesystems (no persistent volume) → cache is lost
       on container restart anyway, falls back to MVP behavior (full resync) —
       no regression. Operators wanting persistence mount a volume.
    5. **Corruption recovery.** Header magic + checksum over the file body. On
       checksum mismatch: log WARN, delete the file, full resync. No partial
       recovery; integrity > completeness.
    6. **Multi-JVM cohabitation on the same host.** Cache directory is keyed by
       `(tenantSlug, serverSlug)` so multiple game servers share the host
       without colliding. File lock (`FileChannel.tryLock`) on the cache
       directory prevents two adapter instances of the same server from racing
       on writes — second instance falls back to no-cache behavior with WARN.
    7. **Initial-sync cost amortization.** Initial-sync (R7) for 12M items at
       ~40s Phase 1 + ~30s Phase 2 chunked publish + Kafka burst is a real
       operator pain on every restart. With cache: ~1s load + diff against
       current DB state (already the cycle's normal path). Net win for
       operators with frequent reboots; no impact for operators who restart
       rarely (cache loaded once and reused indefinitely).

  Conclusion: feasible, well-defined boundaries, post-MVP slice. Risks are
  manageable (schema invalidation is the main one; the fingerprint approach
  handles it cleanly).

## Extension points

- **Alternative scan algorithm** — replace the windowed CRC32 sweep with e.g.
  `BINLOG_TAIL` (realtime CDC consuming MySQL binlog events) by introducing a new
  scan implementation behind a new SPI hook on `EntityMapping` (e.g.
  `default ScanMode scanMode() { return ScanMode.WINDOWED_CRC32; }`). Phase 2 +
  publish path stays identical. Plumb new mode through `EntitySyncTask`. Adds the
  enum back, but only after a real customer demands non-CRC32 semantics (low
  priority).
- **Different hash domain** — add a per-mapping `hashFunction()` hook on
  `EntityMapping`. Replaces `CRC32` SQL with `CRC64` / `XX64` and switches
  `Long2IntOpenHashMap` → `Long2LongOpenHashMap`. Doubled RAM cost; not enabled by
  default. Per-entity granularity makes sense here since hash collision risk is
  entity-specific (rare-update entities tolerate weaker hashes).
- **Composite / non-numeric PK** — adds a parallel `Object2IntOpenHashMap<String>`
  snapshot path; `EntityMapping` advertises PK type via a new `PkType` enum. Engine
  dispatches at task creation. Significant SPI churn — defer until a real customer's
  schema demands it.
- **Per-entity timeout** — Tier-2 SPI default: `mapping.queryTimeout()` with
  default value from engine config. Override per mapping when the entity profile
  demands it.
- **Inbound dynamic config** — `nexus.adapter.sync-config` Kafka consumer wired into
  `CdcEngine.applyConfig(...)` — re-schedules tasks when cadence changes. Adds the
  first inbound consumer to the adapter; track it as its own feature.
