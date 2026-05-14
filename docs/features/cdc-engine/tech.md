# CDC Engine — tech

> Covers: spec.md
> Sibling: [`db-sync/tech.md`](../db-sync/tech.md) — module wiring + Tier-2 SPI shape

## Overview

The engine lives inside `:nx-gs-db-sync-core` under `app.l2nx.gs.db.sync.engine`. It is
wired by `DbSyncModule` once `provider.mappings()` is available (Phase 2): one
`EntitySyncTask` instance per `EntityMapping`, all scheduled onto a single shared
`ScheduledThreadPoolExecutor` (`nx-cdc-pool-<schema>-N`, daemon, sized by
`l2nx.cdc-engine.workers`). The task implements the CRC32 two-phase protocol on every
tick — always in PK-windowed mode — walking N windows back-to-back inside a single
tick (where N is derived from `rowsPerWindow` config and the entity's PK range). It
holds a fastutil `Long2IntOpenHashMap` snapshot per entity and publishes `SyncEvent`s
via `NxKafka` to the topic delivered by `ConnectResponse.syncTopics` for that entity.
Per-entity operational state is kept as a `volatile`-published `EntityStats` snapshot
read by `HeartbeatService` per heartbeat tick.

## Structure

- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/`
    - `CdcEngine.java` — orchestrator: builds ONE shared
      `ScheduledThreadPoolExecutor` sized by `l2nx.cdc-engine.workers`
      (default `max(2, min(entities, cores/2))`) with daemon thread factory
      `nx-cdc-pool-<schema>-N` + uncaughtExceptionHandler. Schedules each
      `EntitySyncTask` as a separate task on the shared pool. Validates
      every provider-supplied SQL identifier against
      `^[A-Za-z_][A-Za-z0-9_]{0,63}$` at start; any violation → engine
      `STATE_FAILED`, no tasks scheduled. Surfaces `EntityStats` via
      `EntityStatsTracker` (R5, R6, R10, R15, R16, R19). `stop()` walks
      every task's `StatementRegistry` and calls `Statement.cancel()` to
      interrupt JDBC queries (Thread.interrupt is ignored by most drivers),
      then `awaitTermination` on the pool — failures logged WARN before
      `shutdownNow()`.
    - `EngineConfig.java` — immutable value bag
      `{tickIntervalSeconds, rowsPerWindow, queryTimeoutSeconds,
      publishFlushSeconds, workers, fetchSize}`;
      `productionChain()` resolves file (path from `-Dl2nx.config-file` or
      cwd `l2nx.properties`) + sysprop fallback (R15). `rowsPerWindow`
      hard-capped at 10_000_000 sanity bound (rejected at engine start).
    - `StatementRegistry.java` — per-task tracker of open `Statement`s;
      registered on creation, deregistered on close. `cancelAll()` invoked
      by `CdcEngine.stop()` to interrupt in-flight JDBC queries via
      `Statement.cancel()`.
    - `ConsistentSnapshotTxn.java` — per-window transaction wrapper. Opens
      `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` on the
      borrowed connection, runs the supplied closure (primary hash + every
      child hash + diff), commits on success. Catches `Throwable` (SQL /
      Runtime / Error paths), rolls back, and rethrows the original
      exception so partial MVCC state never leaks back to the host DB.
    - `ConfigResolutionLogger.java` — single static helper invoked once at
      `CdcEngine.start()`; emits engine globals + per-entity topic resolution
      with `[operator-override | default]` source tags (R16)
    - `EntitySyncTask.java` — `Runnable` scheduled onto the shared pool. One
      cycle = borrow → plan (with snapshot envelope) → bucket snapshot PKs
      into the plan's windows (one pass, binary search) → for-each-window
      {ONE consistent-snapshot transaction wraps Phase1 primary + per-child
      hash, XOR-fold, diff → separate Phase2 transaction wraps primary +
      per-child fetch, mapEntity assemble → publish per PK → two-pass walk
      of `Long2ObjectMap<pk, Future>` for THIS window (done-first then
      deadline-bounded for pending), advance snapshot for acked PKs}
      (R1, R2, R5, R7, R9, R11, R20). Carries an `AtomicBoolean ticking`
      overlap guard so a slow tick does not pile up tasks in the shared
      pool; carries a `StatementRegistry` so `CdcEngine.stop()` can
      cancel in-flight queries. Per-window flush keeps cycle-resident
      memory bounded by `rowsPerWindow` — at 6.5M items × default 500K
      window the peak `inFlight` / `pending*` heap drops from ~500 MB
      (cycle-level flush) to ~40 MB (window-level).
    - `SnapshotStore.java` — `Long2IntOpenHashMap` per entity, initialised
      with `defaultReturnValue(Integer.MIN_VALUE)` so the constant
      `MISSING_HASH = Integer.MIN_VALUE` cleanly distinguishes "absent
      key" from a real CRC32 of `0x00000000`. Open-hash chosen over AVL
      tree so the per-entry footprint stays at ~16 bytes; at 6.5M items
      this is ~100 MB resident vs ~360 MB the AVL tree would hold.
      `keysInRange` runs O(N log W) per cycle via top-of-cycle
      `bucketByWindows(plan)` (one pass over the snapshot, binary search
      on plan boundaries) — drops 12M × 24-window from ~288M iterations
      to ~N. `minPk` / `maxPk` are tracked incrementally on `putCrc` /
      `removeCrc` with lazy memoization; the R2 envelope read is O(1)
      amortised (R2, R4).
    - `EntityStatsTracker.java` — `ConcurrentHashMap<String, EntityStats>` +
      per-entity `AtomicInteger` consecutiveErrors. `recordCycleResult`
      writes `entityOrder` first then `latest` so a heartbeat reader between
      the two writes never drops the entity (R10)
    - `CycleResult.java` — outcome of one `EntitySyncTask.runCycle()`
      surfaced to `EntityStatsTracker`. Counters reflect post-publish-walk
      successful operations only
    - `SafeRunnable.java` — local copy of the same-named utility in
      `:nx-gs-adapter-core`. Duplicated here because db-sync depends only on
      api (cross-module dep avoidance)
    - `phase/`
        - `Phase1Hasher.java` — two methods:
          `hashPrimary(window, primarySource, conn, ...)` runs `SELECT pk,
          CRC32(CONCAT_WS(...)) FROM primary WHERE pk BETWEEN ? AND ?` →
          `Long2IntMap`; `hashChild(window, childSource, conn, ...)` runs
          `SELECT fk, BIT_XOR(CRC32(CONCAT_WS(...))) FROM child WHERE fk
          BETWEEN ? AND ? GROUP BY fk` → `Long2IntMap`. Both share ONE
          window-scoped `ConsistentSnapshotTxn` opened by `EntitySyncTask`
          — primary + every child see the same MVCC view, so a row written
          mid-window can no longer corrupt the per-PK XOR-fold. Every
          `PreparedStatement` calls `Phase1Hasher.applyFetchSize(ps, cfg.fetchSize, dialect)`:
          MySQL/MariaDB (detected via `JdbcDialect.detect`) gets
          `Integer.MIN_VALUE` for row-by-row streaming (the only mode
          Connector/J honors for large result sets); Postgres / other drivers
          get `cfg.fetchSize` (default `10_000`) as a server-side cursor
          batch hint. Each statement is
          registered with the task's `StatementRegistry` so shutdown
          cancellation reaches it. Engine ({@link EntitySyncTask}) XOR-folds
          per-source contributions into a per-window aggregate
          `currentScan` (R1, R11, R20).
        - `Phase2Fetcher.java` — two methods: `fetchPrimary(primarySource,
          pks, conn, ...)` runs `SELECT * FROM primary WHERE pk IN (...)`
          chunked at 1000 → `Long2ObjectMap<Object>` (PK → opaque primary
          row); `fetchChild(childSource, fks, conn, ...)` runs
          `SELECT * FROM child WHERE fk IN (...)` chunked at 1000 →
          `Long2ObjectMap<List<Object>>` (FK → child rows). Single
          `PreparedStatement` per call reused across chunks; last
          (smaller) chunk pads by repeating its final PK so the SQL string
          stays stable across chunks (server-side prep cache hit). The
          whole Phase 2 for a window runs inside its OWN
          `ConsistentSnapshotTxn` — intentionally fresher than Phase 1
          (post-Phase-1 row state). `setFetchSize` + `StatementRegistry`
          registration identical to Phase 1 (R1, R11, R20).
        - `ChangeSet.java` — diff stage: walks `currentScan` (aggregate CRC
          across primary + children) against `SnapshotStore` +
          `prevKeysInRange`, partitions PKs into `created/updated/deleted`
          `LongSet`s. Static `diff(...)` factory uses `LongIterator`
          everywhere — no boxing (R1)
    - `publish/`
        - `SyncEventPublisher.java` — builds `SyncEvent<T>`, encodes Kafka key
          as 8-byte big-endian via `ByteBuffer.allocate(8).putLong(pk)` (matches
          `LongSerializer.serialize` byte-for-byte). Returns
          `CompletableFuture<RecordMetadata>` per publish via the
          `KafkaSender` SAM callback (R12, R13)
        - `KafkaSender.java` — narrow byte[]-keyed SAM. Production impl in
          `DbSyncModule.sendViaNxKafka` bridges to
          `NxKafka.instance().send(topic, byte[], value, callback)`; tests
          substitute a recording fake
        - `TopicResolver.java` — `String resolveTopic(String entityName)` SAM
          bound to `ctx.syncTopics()` snapshot at engine start; cached for
          engine lifetime (R17)
    - `window/`
        - `WindowPlanner.java` — `SELECT MIN(pk), MAX(pk) FROM
          <primary.tableName>` + `SnapshotStore.minPk(entity)` /
          `maxPk(entity)`; partitions the **envelope**
          `[min(minDb, minSnap), max(maxDb, maxSnap)]` into half-open
          windows. Closes the DELETE-at-boundary bug. Overflow-guarded
          against full-BIGINT spans; `MAX_WINDOWS_PER_PLAN = 1_000_000`
          cap protects host JVM from pathological PK ranges (R2)
        - `Window.java` — closed-interval `[fromPk, toPk]` value class
          produced by `WindowPlanner` and consumed by `Phase1Hasher`
- `nx-gs-db-sync-core/build.gradle.kts` — declares `implementation
  it.unimi.dsi:fastutil-core:8.5.15` (primitive maps only — full
  `fastutil` ~21MB NOT pulled) plus `implementation` on `:nx-gs-kafka`
  and `gson`

## Key components

- **`CdcEngine`** (R5, R6, R10, R15, R16, R19) — orchestrator owned by
  `DbSyncModule`. Constructor receives `provider.mappings()` + `JdbcConnectionSource` +
  Kafka producer + `ConfigResolver`. Builds an immutable `EngineConfig` once
  (resolves the R15 chain). Validates every SQL identifier on every
  `PrimarySource` / `ChildSource` against `^[A-Za-z_][A-Za-z0-9_]{0,63}$`
  — first violation transitions the engine to `STATE_FAILED` and aborts
  scheduling. Holds a `Map<EntityMapping<?>, EntitySyncTask>` (provider-list
  order preserved via `LinkedHashMap`) and a single shared
  `ScheduledThreadPoolExecutor` sized by `l2nx.cdc-engine.workers` (daemon
  thread factory `nx-cdc-pool-<schema>-N` with uncaughtExceptionHandler).
  `start()` emits the R16 startup log via `ConfigResolutionLogger`, then
  schedules each `EntitySyncTask` as a separate task on the shared pool
  with first-tick delay 0 and `scheduleWithFixedDelay` at
  `tickInterval`. `stop()` walks every task's `StatementRegistry` calling
  `Statement.cancel()` (Thread.interrupt is ignored by most JDBC drivers
  — only `Statement.cancel()` actually aborts a running query), then
  `awaitTermination` on the pool (failures logged WARN before
  `shutdownNow`). Exposes `List<EntityStats> currentEntityStats()` for
  heartbeat enrichment — reads from a single `volatile` reference rebuilt
  at the end of every cycle by each task.
- **`EngineConfig`** (R15) — immutable value bag carrying ONLY engine-
  level globals from `l2nx.properties`:
  ```java
  Duration tickInterval;            // l2nx.cdc-engine.tick-interval-seconds      | default 60s
  int rowsPerWindow;                // l2nx.cdc-engine.rows-per-window            | default 500_000 (cap 10_000_000)
  Duration queryTimeout;            // l2nx.cdc-engine.query-timeout-seconds      | default 10s
  Duration publishFlush;            // l2nx.cdc-engine.publish-flush-seconds      | default 5s
  int workers;                      // l2nx.cdc-engine.workers                    | default max(2, min(entities, cores/2))
  int fetchSize;                    // l2nx.cdc-engine.fetch-size                 | default 10_000
  EnumMap<EngineParam, SourceTag> sources;   // OPERATOR_OVERRIDE | DEFAULT per param,
                                             // for ConfigResolutionLogger
  ```
  No per-entity entries — every entity uses these same global values. Built by
  `CdcEngine` constructor from `ConfigResolver`; passed by reference to every
  `EntitySyncTask`.

- **Entity → topic map** (R17) — separate immutable
  `Map<String, String> entityTopics` snapshot taken from
  `ConnectResponse.syncTopics` at engine construction (entries the platform
  delivered). Built alongside `EngineConfig` but tracked separately because its
  source is the connect-response, not l2nx.properties. An entity whose
  `entityTopics.get(entityName)` returns `null` is recorded as `null` and
  permanently `DEGRADED` per R17.
- **`ConfigResolutionLogger`** (R16) — static helper invoked once at
  `CdcEngine.start()`. Reads `EngineConfig`, formats the structured log block, emits
  via `NxLog` at `INFO` level on logger `app.l2nx.gs.db.sync.engine.CdcEngine`. Single
  call site; no state.
- **`EntitySyncTask`** (R1, R2, R5, R7, R9, R11) — `Runnable` scheduled on
  the shared pool. Each task carries an `AtomicBoolean ticking` overlap
  guard: if a previous tick is still in flight when the next fire arrives
  (slow Phase 1 on a 12M-row entity, say), the new fire short-circuits
  with a single WARN log so the shared pool does not pile up tasks.
  Tracks open statements via a `StatementRegistry` for shutdown
  cancellation. On each tick:
    1. `WindowPlanner.plan(mapping, conn, snapshot, cfg.rowsPerWindow)` —
       runs `SELECT MIN(<pk>), MAX(<pk>) FROM <tableName>`, reads the
       snapshot's incrementally-tracked `minPk` / `maxPk` (O(1)),
       computes the envelope and partitions into `[fromPk, toPk]` windows.
    2. `snapshot.bucketByWindows(plan)` — one pass over the snapshot
       buckets PKs into the plan's windows (binary search) so per-window
       `keysInRange` lookups become O(1) hash hits instead of O(N)
       scans.
    3. For each window in order: ONE `ConsistentSnapshotTxn` wraps
       `Phase1Hasher.hashPrimary` + every `Phase1Hasher.hashChild`,
       XOR-fold into `currentScan`, run `ChangeSet.diff` → if non-empty
       change set, a SEPARATE Phase 2 transaction wraps
       `Phase2Fetcher.fetchPrimary` + every `Phase2Fetcher.fetchChild`
       and the `mapEntity` assemble → `SyncEventPublisher.publish` per
       row + per deleted PK → two-pass walk of the per-window
       `Long2ObjectMap<pk, Future>` (done-first then deadline-bounded
       for pending) → advance `SnapshotStore` for acked PKs only.
    4. After all windows: build a new `EntityStats`, atomically replace
       the published snapshot.
       Wrapped in `SafeRunnable` so any throwable is caught, the entity
       state transitions to `DEGRADED`, and the schedule continues. Small
       entities (rowCount ≤ rowsPerWindow) yield exactly 1 window — no
       separate code path.
- **`Phase1Hasher`** (R1, R11, R20) — runs INSIDE a window-scoped
  `ConsistentSnapshotTxn` owned by `EntitySyncTask` (primary + every
  child for the window share one MVCC view; the transaction commits
  after the diff stage). Two windowed-query variants:
    - `hashPrimary(window, primary, conn, ...)`: `SELECT <pkColumn>,
      CRC32(CONCAT_WS(',', <hashedColumns>)) FROM <primary.tableName>
      WHERE <pkColumn> BETWEEN ? AND ?`. Reads PK via `rs.getLong(1)` and
      CRC32 via `(int) rs.getLong(2)` (CRC32 fits in unsigned 32-bit;
      signed `int` carries the same bytes, comparison is bit-exact). Result:
      `Long2IntOpenHashMap` (PK → primary CRC32).
    - `hashChild(window, child, conn, ...)`: `SELECT <fkColumn>,
      BIT_XOR(CRC32(CONCAT_WS(',', <hashedColumns>))) FROM <child.tableName>
      WHERE <fkColumn> BETWEEN ? AND ? GROUP BY <fkColumn>`. Reads FK via
      `rs.getLong(1)` and aggregate CRC via `(int) rs.getLong(2)`. Result:
      `Long2IntOpenHashMap` (parent PK → XOR aggregate of child CRC32s).
      Empty FK groups are not returned by `GROUP BY` — orphan handling
      happens at fold time in `EntitySyncTask`.

  Both calls `Statement.setQueryTimeout` from
  `l2nx.cdc-engine.query-timeout-seconds` and
  `Statement.setFetchSize(cfg.fetchSize)` so the driver streams rows in
  cursor mode (default batch 10_000). Each statement is registered with
  the task's `StatementRegistry` so `CdcEngine.stop()` can cancel it.
  Closes resultset / statement in try-with-resources. The window-scoped
  transaction commits in `ConsistentSnapshotTxn` once the diff stage
  finishes.

- **`Phase2Fetcher`** (R1, R11, R20) — runs INSIDE a SEPARATE
  `ConsistentSnapshotTxn` (post-Phase-1 fresh view). Two chunked-IN variants:
    - `fetchPrimary(primary, pks, conn, ...)`: `SELECT * FROM
      <primary.tableName> WHERE <pkColumn> IN (?, ?, ...)` chunked at 1000.
      Returns `Long2ObjectMap<Object>` (PK → opaque row produced by
      `primary.mapRow(rs)`).
    - `fetchChild(child, fks, conn, ...)`: `SELECT * FROM
      <child.tableName> WHERE <fkColumn> IN (?, ?, ...)` chunked at 1000.
      Each row produces an opaque object via `child.mapRow(rs)`; engine
      groups by FK. Returns `Long2ObjectMap<List<Object>>` (FK →
      possibly-empty list of child rows). FK absence in the result map
      means "no children for this PK" — caller substitutes `emptyList()`.

  All Phase 2 statements share the SEPARATE consistent-snapshot transaction
  opened for the window (not Phase 1's). PKs/FKs bound via `setLong(...)`.
  Statements use `setFetchSize` + are registered with `StatementRegistry`
  for shutdown cancellation, same as Phase 1.

- **`SnapshotStore`** (R4, R2) — wraps a `Long2IntOpenHashMap` per entity,
  initialised with `defaultReturnValue(Integer.MIN_VALUE)` and exposing
  `MISSING_HASH = Integer.MIN_VALUE` as a public constant (caller compares
  against it instead of `containsKey`-guarding every read). Thread-confined
  to the task thread (no synchronization needed). Provides
  `bucketByWindows(plan)` (called once per cycle, fills a per-window
  `Long2IntMap` cache via one pass over the snapshot + binary search on
  plan boundaries) and `keysInRange(entity, fromPk, toPk)` for the diff
  stage (reads from the per-cycle cache, O(1) per window). `minPk` /
  `maxPk` are tracked incrementally on `putCrc` / `removeCrc` with lazy
  memoization — O(1) amortised. Trade-off is heap (open-hash ~16 B/entry
  vs AVL-tree ~56 B/entry — at 6.5M items: ~100 MB vs ~360 MB) for CPU.
  `containsCrc` / `getCrc` for diff lookups; `putCrc` / `removeCrc` for
  per-row snapshot advance. Wiped on `DbSyncModule.onDisconnect`. NO RAM
  cap enforcement — operator sizes the host JVM to fit the configured
  entities.
- **`ChangeSet`** (R1) — value bag with three `LongSet` (fastutil
  `LongOpenHashSet`). Computed in one pass over the previous-window PKs ∪ current-
  window PKs.
- **`WindowPlanner`** (R2) — runs
  `SELECT MIN(<pkColumn>), MAX(<pkColumn>) FROM <primary.tableName>` at the
  start of each cycle (recompute, NOT cached at connect — auto-extends to
  capture inserts above the prior MAX), then reads
  `SnapshotStore.minPk(entity)` / `maxPk(entity)` to compute the envelope
  `[min(minDb, minSnap), max(maxDb, maxSnap)]`. Computes `windowCount = max(1,
  ceil((maxEnv - minEnv + 1) / rowsPerWindow))`, divides `[minEnv, maxEnv]`
  into evenly-sized half-open intervals, returns `List<Window>`. Both DB
  and snapshot empty → `Collections.emptyList()` (no work this cycle); one
  side empty → use the other; both populated → enveloped union. Closes
  the DELETE-at-boundary correctness gap.
- **`TopicResolver`** (R17) — SAM `String resolveTopic(String entityName)`
  backed by an immutable `Map<String, String>` snapshot taken from
  `ConnectResponse.syncTopics` at engine construction. Returns `null` for entities
  the platform did not deliver a topic for. Engine resolves once per entity at
  start; result stored in `ResolvedEntityConfig.topic`. Null-topic entities are
  permanently `DEGRADED`.
- **`EntityStatsTracker`** (R10) — assembles an `EntityStats` per cycle:
  `name = mapping.entityName()` (the domain identifier — `"clan"`, NOT the source
  table `"clan_data"`), `state` (`HEALTHY` / `DEGRADED`), `rowCount` (cumulative
  Phase 1 size across all windows of the cycle), `lastSyncEpochMs`
  (`Instant.now().toEpochMilli()` at successful end of cycle),
  `lastCycleDurationMs` (`System.nanoTime` delta), `lastCycleChanges`
  (`ChangesSummary` from change-set sizes summed across windows),
  `consecutiveErrors` (counter reset on HEALTHY). Published into a
  `volatile List<EntityStats>` field on `CdcEngine` — heartbeat thread reads it via
  `currentEntityStats()`.
- **`SyncEventPublisher`** (R12, R13) — owns `SyncEvent` envelope
  construction. Kafka key is the row's `long` PK (8 bytes via `LongSerializer`);
  topic is resolved once per entity at engine start via `TopicResolver` (delivered
  via `ConnectResponse.syncTopics`) and stored in the entity's `ResolvedEntityConfig`;
  value is Gson-serialized `SyncEvent { entityName, pk: long, op, payload, timestamp }`
  — `op=DELETED` carries `payload=null` in the JSON envelope but the
  envelope itself is a non-null Kafka value (not a tombstone — db-sync
  topics use bounded retention, not log compaction). Async send via
  `NxKafka.send` — does not block. Tracks per-window in-flight
  `Future<RecordMetadata>`s in a `Long2ObjectMap` keyed by PK; at
  end-of-window the engine walks the map in TWO passes (done-first via
  `isDone()`, then deadline-bounded `f.get(remainingNs, NANOSECONDS)` for
  pending) under the `l2nx.cdc-engine.publish-flush-seconds` (default 5s)
  shared budget. Per-PK successful publishes advance the snapshot; failed
  / timed-out PKs leave the snapshot untouched and replay on the next
  cycle (R13).

## Data flows

### 0. Engine startup — config resolution + log

```
CdcEngine constructor (called by DbSyncModule.start, Phase 2)
  → EngineConfig cfg = resolveEngineConfig(configResolver)
       → cfg.tickInterval            = ... l2nx.cdc-engine.tick-interval-seconds      | 60s
       → cfg.rowsPerWindow           = ... l2nx.cdc-engine.rows-per-window            | 500_000 (cap 10_000_000)
       → cfg.queryTimeout            = ... l2nx.cdc-engine.query-timeout-seconds      | 10s
       → cfg.publishFlush            = ... l2nx.cdc-engine.publish-flush-seconds      | 5s
       → cfg.workers                 = ... l2nx.cdc-engine.workers                    | max(2, min(entities, cores/2))
       → cfg.fetchSize               = ... l2nx.cdc-engine.fetch-size                 | 10_000
       → cfg.sources tagged per param (OPERATOR_OVERRIDE if present in props, else DEFAULT)
       (NB: MySQL/MariaDB ignore positive fetchSize; the engine auto-detects
        dialect via JdbcDialect.detect at first borrow and switches to
        Integer.MIN_VALUE streaming on those drivers.)
  → validateIdentifiers(provider.mappings())     -- R19: ^[A-Za-z_][A-Za-z0-9_]{0,63}$
                                                   on every tableName / pkColumn / fkColumn /
                                                   hashedColumns entry; first violation →
                                                   STATE_FAILED
  → entityTopics = unmodifiableCopy(ctx.syncTopics())   -- snapshot from connect-response
  → pool = new ScheduledThreadPoolExecutor(cfg.workers, nxCdcPoolFactory)
            with uncaughtExceptionHandler

CdcEngine.start()
  → ConfigResolutionLogger.log(cfg, provider.mappings(), entityTopics)
                                                -- R16, single contiguous INFO block
  → for each mapping in provider.mappings() (linked, order preserved):
        String topic = entityTopics.get(mapping.entityName())
        if (topic == null) {
            stats.markDegraded(mapping.entityName(), "no topic in connect response")
            continue                            -- R17: permanent DEGRADED, no schedule
        }
        pool.scheduleWithFixedDelay(taskFor(mapping, topic), 0, cfg.tickInterval, ...)

CdcEngine.stop()
  → for each task: task.statementRegistry().cancelAll()   -- Statement.cancel on
                                                            in-flight JDBC queries
  → pool.shutdown(); awaitTermination(...)
  → on timeout: WARN, pool.shutdownNow()
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

### 1. Cycle (multi-source windowed strategy)

```
EntitySyncTask.run()  [shared pool worker, daemon]
  → if (!ticking.compareAndSet(false, true)) {
       WARN "previous tick still running, skipping"; return
    }
  → cycleStart = System.nanoTime()
  → try (Connection conn = jdbcSource.getConnection()) {
      windows = WindowPlanner.plan(mapping, conn, snapshot, cfg.rowsPerWindow)
                          -- envelope [min(minDb, minSnap), max(maxDb, maxSnap)]
                          -- minPk/maxPk read in O(1) (incrementally tracked)
      snapshot.bucketByWindows(windows)           -- one pass, binary search;
                                                    populates per-window cache for
                                                    O(1) keysInRange lookups
      totalCreated = totalUpdated = totalDeleted = 0
      for (Window w : windows) {
          // Phase 1 — ONE consistent-snapshot txn covers primary + every child
          ConsistentSnapshotTxn.runReadOnly(conn, () -> {
              Long2IntMap currentScan = Phase1Hasher.hashPrimary(w, primary, conn)
              for (ChildSource c : children) {
                  Long2IntMap childHash = Phase1Hasher.hashChild(w, c, conn)
                  for (entry in childHash) {
                      if (currentScan.get(entry.fk) != MISSING_HASH) {
                          currentScan.put(entry.fk, currentScan.get(entry.fk) ^ entry.xorCrc)
                      }
                      -- orphan FKs (no primary row) dropped silently
                  }
              }
              return ChangeSet.diff(currentScan, snapshot.keysInRange(w), snapshot, entity)
          })
          if (!diff.empty()) {
              // Phase 2 — SEPARATE consistent-snapshot txn (fresh post-Phase-1 view)
              ConsistentSnapshotTxn.runReadOnly(conn, () -> {
                  Long2ObjectMap<Object> primaryRows = Phase2Fetcher.fetchPrimary(primary, createdUpdated, conn)
                  Map<String, Long2ObjectMap<List<Object>>> childRowsByTable = {}
                  for (ChildSource c : children) {
                      childRowsByTable.put(c.tableName(),
                              Phase2Fetcher.fetchChild(c, createdUpdated, conn))
                  }
                  for pk in createdUpdated:
                      Map<String, List<Object>> children = collect(childRowsByTable, pk)
                      T dto = mapping.mapEntity(primaryRows.get(pk), children)
                      publish(op, pk, dto)              -- async, future captured per PK
                  for pk in diff.deleted:
                      publish(DELETED, pk, null)
              })
              walkInFlightAndAdvance(...)         -- two-pass per-window flush:
                                                    1. drain Future.isDone() (cheap)
                                                    2. f.get(remainingNs, NS) for pending
                                                    -- per-PK snapshot swap on Kafka ack
          }
          totalCreated += diff.created.size()
          totalUpdated += diff.updated.size()
          totalDeleted += diff.deleted.size()
      }
      stats.markHealthy(totalCreated, totalUpdated, totalDeleted)
    } catch (Throwable t) {
      stats.markDegraded(t)                         -- snapshot NOT advanced for the
                                                    -- failing window
    } finally {
      ticking.set(false)
      stats.lastCycleDurationMs = (System.nanoTime() - cycleStart) / 1_000_000
      engine.publishStatsSnapshot()                 -- atomic List<EntityStats> rebuild
    }
```

Small entities (rowCount ≤ rowsPerWindow): `windows` returns a single
`[minEnv, maxEnv]` interval — the loop runs once, operationally identical to a
"full scan". Single-table entities (`children = []`): the per-child loops
are no-ops; behaves like a pure-primary CRC32 cycle.

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
- **Per-window consistent snapshot for Phase 1, SEPARATE per-window
  transaction for Phase 2.** Primary + every child for one window share
  ONE `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` so the
  per-PK XOR-fold sees a single MVCC view. Per-query transactions
  (earlier design) left a race window between the primary scan and the
  child scans: a row written mid-window could be primary-hashed pre-
  write and child-hashed post-write, producing spurious UPDATED events
  on the next cycle. Phase 2 runs in its OWN consistent-snapshot
  transaction — intentionally fresh: a row deleted between phases is a
  silent no-op for this cycle and detected next cycle. Wrapping the
  whole entity cycle (N×Phase 1 windows + N×Phase 2 chunks) in one
  REPEATABLE READ would hold a multi-second-to-multi-minute transaction
  on the host DB, growing InnoDB's undo log and slowing the game core's
  writes — the explicit Non-goal in `db-sync`. Per-window snapshot keeps
  each window internally consistent without the long-tx penalty.
- **Dialect-aware JDBC fetch.** `JdbcDialect.detect` reads
  `Connection.getMetaData().getURL()` once per task and routes
  `applyFetchSize` accordingly: MySQL/MariaDB → `Integer.MIN_VALUE`
  (row-by-row streaming, the only mode Connector/J honors for large
  result sets — positive fetchSize is silently ignored); Postgres /
  other → `cfg.fetchSize` as a server-side cursor batch on
  `autoCommit=false`. Without this, a 12M-row Phase 1 scan on a
  MySQL host buffers every row into the client heap and OOMs the JVM
  before the snapshot map fills.
- **Statement.cancel() on shutdown, not Thread.interrupt().** Most JDBC
  drivers ignore thread interruption and leave a multi-minute Phase 1
  scan running until it completes naturally. The only portable way to
  abort an in-flight JDBC query is `Statement.cancel()`, which the
  engine wires via a per-task `StatementRegistry` walked by
  `CdcEngine.stop()`.
- **MIN/MAX recompute at start of every cycle, NOT cached at connect.** Cached
  boundaries miss rows inserted above the prior MAX — for an auto-increment PK like
  `items.id` this is a constant correctness bug (every game-server tick produces new
  `items` rows). The recompute query (`SELECT MIN(pk), MAX(pk) FROM tbl`) uses the PK
  index → O(log n) two index lookups, < 50ms even on 12M rows. The cost is negligible
  compared to the Phase 1 scan (20–40s).
- **Envelope-based windowing for DELETE correctness.** The window range is the
  union of `[MIN_db, MAX_db]` and `[MIN_snapshot, MAX_snapshot]`, NOT just
  `[MIN_db, MAX_db]`. Without the envelope, deleting the row at the current
  `MIN(pk)` or `MAX(pk)` shrinks the DB range and pushes the deleted PK
  outside every next-cycle window — its tombstone never fires and stale state
  lives in the platform indefinitely. AVL-tree-backed `SnapshotStore` exposes
  `firstLongKey` / `lastLongKey` in O(log N), so the envelope read is
  sub-millisecond regardless of snapshot size. The cost (slightly larger
  envelope when snapshot has historical reach beyond DB extremes) is bounded
  by snapshot-vs-DB drift, which converges back to `[MIN_db, MAX_db]` once
  the envelope's tombstones publish and the snapshot keys are removed.
- **Multi-source assembly via PrimarySource + ChildSource, no JOINs.** Each
  `EntityMapping<T>` declares one primary (drives windowing + identity) and
  zero-or-more children (FK back to primary's PK). Engine emits one SQL
  statement per source — never a JOIN — so the SPI stays agnostic about
  per-tenant column aliases / ON conditions. Aggregation happens in-engine:
  primary CRC XOR-folded with each child's `BIT_XOR(CRC32(...))` group
  aggregate. Per-source rows are opaque `Object` to the engine; impl casts
  inside `mapEntity` to its own private row types.
- **`BIT_XOR(CRC32(...))` for child aggregation.** Order-insensitive,
  associative — child row order in the source table has no bearing on the
  hash. Implementation cost: one `GROUP BY fk` query per child source. The
  collision risk for two child rows with identical CRC32 inside the same FK
  group cancelling in XOR is `~1/2^32` per pair — for an entity with N child
  rows the per-cycle change-miss probability is bounded by `N(N-1)/2 × 1/2^32`
  and is acceptable for game-data eventual consistency. Mitigation
  (`XOR COUNT(*)` row-count guard) considered: includes the row count in the
  aggregate so `add then remove of identical rows` produces a different hash
  than baseline. Deferred — adds noise without a real collision pattern
  observed; if M21 e2e or M22 manual smoke surfaces collisions, the guard
  is purely additive on the wire (snapshot CRC values change once on
  rollout, behaves like an initial-sync replay for affected entities).
- **Shared `ScheduledThreadPoolExecutor`, NOT thread-per-entity.** Earlier
  design ran one daemon thread per `EntityMapping`; at 10+ entities on
  hosts with 4-core JVM allowances this scaled poorly (kernel scheduling
  thrash, GC root-set bloat). The shared pool is sized by
  `l2nx.cdc-engine.workers` (default `max(2, min(entities, cores/2))`)
  with daemon factory `nx-cdc-pool-<schema>-N`. Per-entity state stays
  thread-confined in practice via the `AtomicBoolean ticking` overlap
  guard — a task that runs longer than `tick-interval-seconds` skips its
  next fire with a WARN instead of running two ticks concurrently for
  the same entity. The guard also prevents slow entities from piling
  tasks in the pool queue and starving other entities.
- **SQL identifier validation at engine start.** Engine interpolates
  provider-supplied identifiers directly into SQL (`CRC32(CONCAT_WS(',',
  <hashedColumns>))`, `BETWEEN ? AND ?` against `<pkColumn>`, etc.);
  parameter binding only covers literal values. A single regex check
  (`^[A-Za-z_][A-Za-z0-9_]{0,63}$`) at `CdcEngine.start()` is enough to
  enforce "bare identifier" contract for `DbSchemaProvider` authors —
  schema-qualified, back-tick-quoted, or hyphenated names are rejected
  with `STATE_FAILED`. Cheap, fail-loud.
- **Two-pass walk-in-flight.** End-of-window publish-flush walks the
  in-flight `Long2ObjectMap` in two passes: first `Future.isDone()` to
  drain easy outcomes, then deadline-bounded `f.get(remainingNs, NS)`
  for pending. Single-pass blocking on each future in order could let
  one slow ack at the head of the queue starve later already-acked
  publishes against the shared `publishFlushSeconds` budget; two
  passes guarantee the budget covers actual pending work, not bookkeeping.
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
