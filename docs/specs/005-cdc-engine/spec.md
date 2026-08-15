# CDC Engine

## Problem

`db-sync` (and any future DB-reading module) need a generic, lightweight Change Data Capture
algorithm to ship row-level changes from the host game-server's MySQL into Kafka — without
binlog access, triggers, or schema modifications, with `GRANT SELECT` only on the host DB.
The engine must be agnostic to which schema is being synced: it consumes a list of
`EntityMapping` (Tier-2 SPI shape lives in `db-sync` feature) and applies the same protocol
to every entity.

This slice extracts the CDC algorithm out of `db-sync` into its own design surface.
`db-sync` keeps the `AdapterModule` contract, the Tier-2 SPI (`DbSchemaProvider` /
`EntityMapping`), and the bohpts MVP wiring. `cdc-engine` owns the algorithm: CRC32 two-phase
protocol, scheduling, in-memory hash snapshot, change publication, per-entity stats reporting,
RAM/timeout safety nets. The engine has no knowledge of any specific entity or schema; it is
fed `EntityMapping`s by `db-sync`'s `DbSchemaProvider` resolver.

Audience: db-sync engine authors (own this code path), future DB-reading module authors
(`metrics-db`, etc. — same engine, different schema providers), platform-side consumers of
`gs.sync.*` Kafka topics (rely on the wire semantics this slice defines), operators (observe
per-entity state via heartbeat enrichment).

## Requirements

> **Phase split:** The entire engine lands in `db-sync` Phase 2. There is no Phase 1
> engine work — Phase 1 of `db-sync` is SPI plumbing only (smoke borrow + heartbeat pool
> stats).

> **Sibling specs in scope:**
> - [`db-sync`](../003-db-sync/spec.md) provides `EntityMapping` / `DbSchemaProvider` (Tier-2
    > SPI shape) — the engine accepts these uniformly, no engine code knows about specific
    > entities.
> - [`adapter-modules`](../002-adapter-modules/spec.md) defines `ModuleStatus` / `Stats` /
    > `PoolStats` / `EntityStats` / `EntityState` / `ChangesSummary` types — the engine
    > populates `Stats.entities[]` per cycle.
> - [`jdbc-connection-source`](../004-jdbc-connection-source.md) provides
    > `JdbcConnectionSource` — the engine borrows connections through it. The engine
    > does NOT call `setReadOnly(true)` on borrowed connections; read-only is enforced
    > at the SQL level via `START TRANSACTION ... READ ONLY` (see Single Consistent
    > Snapshot below).

**Must:**

- [wip] R1. The engine MUST execute the CRC32 two-phase protocol per `EntityMapping`
  on every scheduled tick. The protocol runs in PK-windowed mode (R2) — there
  is no separate "full scan" code path; small entities collapse to a single window
  naturally. An entity is composed of one **primary source** (drives windowing +
  identity) and zero-or-more **child sources** (each carries an FK back to the
  primary's PK). Per cycle, per window:
    - **Single consistent snapshot per window (Phase 1).** Primary + every child
      for one window share ONE borrowed `Connection` and ONE `START TRANSACTION
      WITH CONSISTENT SNAPSHOT, READ ONLY`. The transaction is opened once
      before the primary query, every child runs against the same MVCC view,
      and the transaction is committed (or rolled back) once the diff stage
      finishes. Earlier per-query transactions left a race window between
      primary and child scans — rows written mid-window could be primary-hashed
      pre-write and child-hashed post-write, corrupting the per-PK aggregate
      CRC and emitting false UPDATED events on the next cycle. Phase 2 runs in
      its own (separate) `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ
      ONLY` — fresh post-Phase-1 data is intentional (a row deleted between
      phases is a silent no-op for the current cycle and detected next cycle).
    - **Phase 1 (detect) — primary:** `SELECT <pkColumn>, CRC32(CONCAT_WS(',',
      col1, col2, ...)) FROM <primary.tableName> WHERE <pkColumn> BETWEEN ? AND ?`.
      MySQL computes hashes server-side; engine reads PK as `long` and CRC32 as
      `int` into a fastutil `Long2IntOpenHashMap` (`primaryHash`).
    - **Phase 1 (detect) — each child source:** `SELECT <fkColumn>,
      BIT_XOR(CRC32(CONCAT_WS(',', col1, col2, ...))) FROM <child.tableName>
      WHERE <fkColumn> BETWEEN ? AND ? GROUP BY <fkColumn>`. Returns
      FK → XOR-aggregated CRC32 of every child row that belongs to the parent
      PK. `BIT_XOR` is order-insensitive and associative — child row order
      does not affect the aggregate.
    - **Aggregate:** for every PK present in `primaryHash`, fold its primary CRC
      with each child's XOR-CRC for the same PK via XOR:
      `entityCrc[pk] = primaryHash[pk] ^ child1Hash.getOrDefault(pk, 0) ^
      child2Hash.getOrDefault(pk, 0) ^ ...`. PKs that appear in a child but
      have no matching primary row (orphan FKs) are dropped silently — the
      entity does not exist without a primary row.
    - **Diff:** the aggregated `entityCrc` for the window vs the snapshot for the
      same PK range → `{ created: LongSet, updated: LongSet, deleted: LongSet }`.
      Created = present in current, absent in previous; deleted = inverse;
      updated = present in both with different aggregate CRC32. Only PKs in
      `[fromPk, toPk]` are evaluated — PKs outside the window stay untouched in
      the persistent snapshot.
    - **Phase 2 (fetch) — primary:** `SELECT * FROM <primary.tableName> WHERE
      <pkColumn> IN (?, ?, ...)` for `created ∪ updated`, chunked at 1000 PKs per
      query. PKs bound via `setLong(...)`. Each row mapped via
      `primary.mapRow(rs)` to an opaque per-source row object.
    - **Phase 2 (fetch) — each child source:** `SELECT * FROM <child.tableName>
      WHERE <fkColumn> IN (?, ?, ...)` for the same `created ∪ updated` PKs,
      chunked identically. Rows mapped via `child.mapRow(rs)`; engine groups
      results by FK so each parent PK gets a (possibly empty) list of child
      row objects per child source.
    - **Assemble:** for every PK in `created ∪ updated`, the engine calls
      `mapping.mapEntity(primaryRow, childRowsByTable)` where
      `childRowsByTable: Map<String, List<Object>>` is keyed by
      `child.tableName()`. The mapping casts the opaque rows back to its
      impl-private types and returns the typed entity DTO `T`.
    - **Publish:** `(mapping, op, pk, dto|null)` translated to a `SyncEvent` and
      pushed via the Kafka producer initialized by `adapter-bootstrap`. PK is
      `long` end-to-end — engine internals (fastutil maps, JDBC
      `setLong`/`getLong`), wire payload (`SyncEvent.pk: long`, JSON number on
      the wire), and Kafka key (binary `LongSerializer`, 8 bytes) all carry the
      raw long. No stringification anywhere — platform stores the PK as `long`
      and reads it back as `long`. Topic name comes from
      `ConnectResponse.syncTopics[entityName]` (per
      [`adapter-bootstrap` R16](../001-adapter-bootstrap.md)).
    - **Per-row snapshot swap:** the in-memory `Long2IntOpenHashMap` for the
      entity is advanced **per PK** at end-of-window (within
      `publish-flush-seconds`), not per cycle. Each Phase-2 publish records
      its `Future<RecordMetadata>` keyed by PK in a window-local in-flight
      map; once Phase-2 publishing for the window finishes, the engine walks
      the map and advances `SnapshotStore` only for PKs whose publish
      succeeded (created/updated → put aggregated CRC32 from current scan;
      deleted → remove). PKs whose publish failed (or timed out) are left
      untouched in the previous snapshot, so the next cycle's diff
      re-detects them and replays the publish. PKs outside the current
      window's `[fromPk, toPk]` are never touched. Per-window flush keeps
      cycle-resident `inFlight` / `pending*` heap bounded by `rowsPerWindow`
      rather than by total snapshot size — at items-scale (6.5M+ rows ×
      default 500K window) this caps the accumulators at ~40 MB instead of
      ~500 MB if they were carried across all 13 windows of one cycle.

- [wip] R2. **Single windowed sync strategy with envelope-based windowing.** There is
  one strategy: PK-range windowed scan. The engine partitions the entity's PK range
  into windows and walks them sequentially within one cycle, back-to-back, no pause:
    - At the start of every cycle the engine runs `SELECT MIN(<pk>), MAX(<pk>) FROM
      <primary.tableName>` (recompute, NOT cached at connect — auto-extends to
      capture inserts above prior MAX).
    - **Envelope rule (DELETE-correctness)** — the windowed range covers the union
      of the live DB range AND the snapshot's PK range:
        - `minDb` / `maxDb` from the `SELECT MIN/MAX` query (both empty when the
          primary table is empty).
        - `minSnap` / `maxSnap` from `SnapshotStore.minPk(entityName)` /
          `maxPk(entityName)` (both empty on initial cold cycle).
        - Both empty → no windows this cycle, return early.
        - One side empty → use the other.
        - Both populated → `minEnv = min(minDb, minSnap)`, `maxEnv = max(maxDb,
          maxSnap)`. Partition `[minEnv, maxEnv]` (NOT `[minDb, maxDb]`).
          Rationale: when an extreme PK is deleted (e.g. the row at current `MAX(pk)`),
          `maxDb` shrinks below the deleted PK; without the envelope, that PK falls
          outside every window of the next cycle and its DELETE never fires. The
          envelope keeps every PK ever seen in the snapshot in scope until its
          tombstone is published and its CRC removed.
    - PK range size = `maxEnv - minEnv + 1`. Window count = `max(1, ceil(pkRange /
      rowsPerWindow))`. Window boundaries = even subdivision of `[minEnv, maxEnv]`
      into that many half-open intervals.
    - `rowsPerWindow` comes from `l2nx.cdc-engine.rows-per-window` (default 500_000)
      per R15. One global value applied uniformly to every entity.
    - Small entities (rowCount <= rowsPerWindow): yield exactly 1 window covering the
      entire enveloped PK range — operationally identical to a "full scan" without a
      separate code path.
    - Large entities (e.g. 12M items, 500k rows-per-window default → 24 windows):
      sequential walk through every window inside one cycle.
    - Sparse-PK entities (auto-increment + many deletions, large gaps in PK range):
      window count over-estimates relative to actual row count — windows simply contain
      fewer rows than `rowsPerWindow`. Operationally fine; predictable.
    - The single in-memory `Long2IntOpenHashMap` per entity covers all windows of that
      entity (per R4) — only the window currently being scanned is touched per
      iteration. `SnapshotStore.minPk` / `maxPk` are O(N) full-key scans called
      once per cycle (envelope planning) — at 6.5M items ~50 ms, dwarfed by the
      DB scan itself.
    - SC1. MIN/MAX recompute query completes in < 50ms even on the largest target
      entity (~12M items) on a host with PK index intact.

> **R3 (formerly `SLIDING_WINDOW` strategy as a separate mode) — folded into R2.**
> Single-strategy decision: removed the `SyncStrategy { FULL_SCAN | SLIDING_WINDOW }`
> enum and `EntityMapping.strategy()` / `EntityMapping.windowCount()` fields. The
> "small entity = 1 window, large entity = N windows" semantics fall out of R2's
> rowsPerWindow math without a per-entity strategy switch. Number R3 is intentionally
> left as a gap.

- [wip] R4. The engine MUST hold each entity's previous-snapshot in a fastutil
  `it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap` (PK → aggregate CRC32).
  One map per `EntityMapping`, lifetime = adapter lifetime. Wiped on
  `DbSyncModule.onDisconnect`. Open-hash (rather than AVL tree) is required
  so the per-entry footprint stays at ~16 B — at items scale (6.5M+ rows)
  this is ~100 MB resident vs ~360 MB the AVL tree would burn.
  `fastutil-core` (~3 MB JAR; primitive maps only — full `fastutil` ~21 MB
  is NOT pulled) is added as a runtime dep on `nx-gs-db-sync-core`.

  **Hash sentinel.** Each map is initialised with
  `defaultReturnValue(Integer.MIN_VALUE)` and the same constant is exposed as
  `SnapshotStore.MISSING_HASH`. Removes the historical "is `0` an absent key
  or a real CRC32 of `0x00000000`" ambiguity at the type level — callers
  compare against `MISSING_HASH` instead of `containsKey`-guarding every
  read.

  **`keysInRange` performance.** A naive O(N) full-key scan per window blew
  up at 12M rows × 24 windows (~288M iterations per cycle). The store now
  buckets PKs into the current plan's windows in one pass at top-of-cycle
  (binary search on sorted boundaries) — total per-cycle work is O(N log W)
  where W = window count, i.e. ~N total iterations.
  `minPk` / `maxPk` are tracked incrementally on every `putCrc` /
  `removeCrc` with lazy memoization, so the R2 envelope read is O(1)
  amortised. No more per-cycle O(N) min/max scans.
    - SC2. For 12M entries, RAM occupancy of one snapshot stays under 200 MB measured via
      `Runtime.totalMemory() - Runtime.freeMemory()` delta around the snapshot population.

- [wip] R5. The engine MUST run every `EntityMapping` on a single SHARED
  `ScheduledThreadPoolExecutor` (NOT one daemon thread per entity):
    - Pool size = `l2nx.cdc-engine.workers` (default `max(2, min(entities,
      cores/2))`); per-entity threads scaled poorly at 10+ entities on hosts
      with 4-core JVM allowances.
    - Thread factory produces daemon threads named
      `nx-cdc-pool-<schemaName>-<N>` with an uncaughtExceptionHandler that
      logs and continues (the pool itself is non-fatal — an entity error
      never tears down the worker).
    - First tick fires immediately after `DbSyncModule.start` (initial sync — see R7);
      subsequent ticks at the engine-global tick interval from
      `l2nx.cdc-engine.tick-interval-seconds` (R15) — every entity ticks at the same
      cadence, scheduled as separate tasks onto the shared pool via
      `scheduleWithFixedDelay`.
    - **Overlap guard.** Each task carries an `AtomicBoolean ticking` — when
      a previous tick is still in flight (e.g. a 12M-row entity that ran
      longer than `tick-interval-seconds`), the next fire is skipped with a
      single WARN log line. Without this guard, slow entities would pile up
      tasks in the pool queue and starve other entities.
    - Tick body wrapped in `SafeRunnable` so an uncaught throwable does not
      cancel the schedule.

- [wip] R6. **Cycle order — provider list.** When `DbSyncModule` configures the
  engine with `provider.mappings()`, the engine launches one scheduler thread per
  entity in the ORDER returned by the provider. Within a single thread, ticks happen
  at the mapping's `tickInterval()`; entities on different threads run independently
  and can overlap. Provider authors are expected to declare small/fast entities first
  if cross-entity cycle ordering matters (informational guideline — engine does not
  enforce or sort by row count).

- [wip] R7. **Initial sync** — first tick after `DbSyncModule.start()` MUST replay
  every existing row as `CREATED` events. Previous snapshot is empty → envelope
  collapses to `[minDb, maxDb]` → diff for each window returns all PKs in that
  window as created → Phase 2 fetches primary + child rows → `mapEntity` assembles
  each entity → publishes one `SyncEvent { op: CREATED }` per row. No special
  bootstrap mode; the engine's normal windowed Phase 1 + Phase 2 path handles
  initial sync naturally — every window's PKs come back as created on the first
  cycle, then nothing on subsequent cycles unless data actually changes.

> **R8 (per-entity snapshot RAM cap) — removed.** The engine does NOT enforce a
> snapshot row-count cap. Operators size the host JVM heap to fit the configured
> entities; an entity that grows beyond expected size simply uses more RAM. Number
> R8 left as a gap. `EntityState.SKIPPED` is therefore also removed (its sole
> producer was the cap path) — `EntityState` enum carries only `HEALTHY | DEGRADED`.

- [wip] R9. The engine MUST apply a global query timeout via
  `Statement.setQueryTimeout(int seconds)` to every Phase 1 and Phase 2 query. Default
  10s, configurable via `l2nx.cdc-engine.query-timeout-seconds`. On
  `SQLTimeoutException`, the current window is aborted, the per-entity state
  transitions to `DEGRADED` for that cycle, the engine continues to the next window
  (or, if it was the last window, the next tick), and the snapshot for the affected
  window is NOT advanced.

  **Dialect-aware fetch.** Every `Statement` / `PreparedStatement` opened by
  Phase 1 / Phase 2 MUST call `setFetchSize(...)`. The driver dialect is
  auto-detected once per entity task from `Connection.getMetaData().getURL()`
  (`JdbcDialect.detect`):
    - **MySQL Connector/J** (`jdbc:mysql:`): `setFetchSize(Integer.MIN_VALUE)` —
      the only mode MySQL Connector/J honors for large result sets. Without it
      the driver buffers the entire result set in client memory (positive
      `fetchSize` is silently ignored). Required for the typical L2 deployment
      where the host runs on MySQL and entities like `items` can hit 12M+ rows.
    - **MariaDB Connector/J** (`jdbc:mariadb:`): `setFetchSize(l2nx.cdc-engine.fetch-size)`
      (default `10_000`). MariaDB Connector/J 3.x validates `fetchSize >= 0` and
      throws `SQLException: invalid fetch size` on the MySQL streaming sentinel,
      so MariaDB is split out as its own dialect. Default behavior buffers the
      result set; add `useCursorFetch=true` to the JDBC URL for true server-side
      cursors.
    - **Postgres** (and other drivers): `setFetchSize(l2nx.cdc-engine.fetch-size)`
      (default `10_000`) — server-side cursor batch on `autoCommit=false`
      transactions (the engine sets `autoCommit=false` inside
      `ConsistentSnapshotTxn`).

  **Statement cancellation on shutdown.** Every open `Statement` is registered
  in a per-task `StatementRegistry`; `CdcEngine.stop()` walks the active tasks
  and calls `Statement.cancel()` on each tracked statement to actually
  interrupt the JDBC query (most JDBC drivers ignore `Thread.interrupt()`, so
  thread interruption alone leaves a multi-minute Phase 1 scan running until
  it completes naturally). `awaitTermination` failures during pool shutdown
  are logged WARN; the pool is then `shutdownNow()`-ed.

- [done] R10. The engine MUST publish per-entity operational state on every cycle into
  `ModuleStatus.Stats.entities[]` (defined in `adapter-modules`). Each `EntityStats`
  entry carries:
    - `name` — entity name (e.g. `"clan"`, NOT source table `"clan_data"`)
    - `state` — `HEALTHY` | `DEGRADED` (`EntityState` enum; `SKIPPED` removed
      together with R8)
    - `rowCount` — last observed via Phase 1
    - `lastSyncEpochMs` — `Instant.toEpochMilli()` of last successful cycle completion
    - `lastCycleDurationMs` — Phase 1 + Phase 2 + publish elapsed
    - `lastCycleChanges` — `ChangesSummary { created, updated, deleted }` counts
    - `consecutiveErrors` — 0 if last cycle clean; incremented on DEGRADED, reset on
      HEALTHY

  Updates to this snapshot are atomic from the heartbeat-thread reader perspective
  (single `volatile` reference to a `List<EntityStats>` rebuilt per cycle).
    - SC3. Heartbeat reader (running on its own thread) never observes torn state —
      every `currentStatuses()` invocation returns a fully populated, consistent
      `entities` list.

- [wip] R11. **Window-scoped InnoDB consistent snapshot.** Per cycle, per
  window, primary + ALL children execute inside ONE
  `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` on a single
  borrowed `Connection` (or equivalent `SET TRANSACTION ISOLATION LEVEL
  REPEATABLE READ` + first-SELECT semantics on MariaDB / MySQL). The
  transaction is committed once the diff stage finishes. Phase 2 for the
  same window opens its own (separate) consistent-snapshot transaction —
  intentional: Phase 2 fetches post-Phase-1 row state, and a row deleted
  between phases is a silent no-op for the current cycle (the next cycle's
  Phase 1 detects the deletion).

  The Phase-1 transaction wrapper catches `Throwable` (SQL / Runtime /
  Error paths) and rolls back before rethrowing the original exception, so
  a buggy CRC fold or fastutil OOM does not leave a half-open MVCC view
  on the host DB.

  Rationale: Phase 1's scan over a 12M-row table can take 20–40s; without
  a consistent snapshot, the scan mixes pre/post-update versions of rows
  and produces false-positive diffs. Per-query transactions left a
  primary↔child race that produced spurious UPDATED events on the next
  cycle — fixed by sharing one transaction across all sources of a window.

- [wip] R15. **Engine config from `l2nx.properties` only — no provider-side
  declarations.** All engine runtime parameters (cadence, window size, timeouts)
  are sourced exclusively from `l2nx.properties` (read via `ConfigResolver` from
  `adapter-bootstrap`); the schema provider does NOT declare any of them.
  `EntityMapping` describes only "what to sync" (entity name, source table, hashed
  columns, row mapper, DTO type) — operational behavior is the operator's
  responsibility, expressed through l2nx config.

  Resolution per parameter is two-step:
    1. **Operator value** — key present in `l2nx.properties`
    2. **Engine default** — hardcoded fallback if key is absent

  **All global engine config keys (MVP):**

  | Key                                              | Type           | Default                            |
                                  |--------------------------------------------------|----------------|------------------------------------|
  | `l2nx.cdc-engine.tick-interval-seconds`          | long, seconds  | 60                                 |
  | `l2nx.cdc-engine.rows-per-window`                | int            | 500_000 (cap 10_000_000)           |
  | `l2nx.cdc-engine.query-timeout-seconds`          | int, seconds   | 10                                 |
  | `l2nx.cdc-engine.publish-flush-seconds`          | int, seconds   | 5                                  |
  | `l2nx.cdc-engine.workers`                        | int            | `max(2, min(entities, cores/2))`   |
  | `l2nx.cdc-engine.fetch-size`                     | int            | 10_000                             |

  Every entity ticks at the same `tick-interval-seconds`, every entity uses the
  same `rows-per-window` partition size, every Phase 1 / Phase 2 query uses the
  same `query-timeout-seconds`. No per-entity overrides, no provider-supplied
  defaults. `rows-per-window` is hard-capped at `10_000_000` — operator
  misconfiguration (e.g. `1_000_000_000`) would defeat the windowing math and
  reintroduce the OOM the design avoids; values above the cap throw
  `IllegalStateException` at engine start.

  **Resolution is one-shot at engine start** — values are cached as `EngineConfig`
  for the lifetime of the engine. Operator changes to `l2nx.properties` require
  adapter restart. Dynamic per-entity reload is post-MVP via R14's inbound Kafka
  consumer.

- [wip] R16. **Rich startup log** — at `CdcEngine.start()`, after R15 resolution
  completes, the engine MUST emit a structured INFO log block listing the resolved
  configuration. Format:

    1. One opening line declaring all engine-level globals + their source:
       `CdcEngine config: tickInterval=60s [default], rowsPerWindow=500000
       [operator-override l2nx.cdc-engine.rows-per-window=500000], queryTimeout=10s
       [default], publishFlush=5s [default]`
    2. One line per entity (in `provider.mappings()` order) listing the entity name
       and its platform-supplied Kafka topic:
       `CdcEngine [clan] → topic=bohpts.gs.sync.clans`
       `CdcEngine [character] → topic=bohpts.gs.sync.characters`
       `CdcEngine [item] → topic=bohpts.gs.sync.items`

  Per-entity lines have NO source tag — there is no per-entity config to attribute;
  the topic is always `connect-response` (single source) and that's implicit. If
  the platform did not deliver a topic for a given entity, the line surfaces it
  explicitly:
  `CdcEngine [item] → topic=<missing — entity DEGRADED>`

  Engine-level globals carry source tags: `operator-override` (key from
  `l2nx.properties` was set) or `default` (hardcoded engine fallback). No
  `provider` source tag — providers don't declare config (see R15).

  Single contiguous block emitted on one logger
  (`app.l2nx.gs.db.sync.engine.CdcEngine`) so operators can grep one log line per
  entity on startup. NO per-row dump, NO repeated emission on subsequent ticks —
  startup-only.
    - SC4. Every entity from `provider.mappings()` appears in the startup log
      exactly once, in provider order.
    - SC5. Engine-level config line carries an explicit `[operator-override |
      default]` source tag per parameter — operators can audit "what is actually
      running" without reading code.

- [wip] R19. **SQL identifier validation at engine start.** Every provider-
  supplied identifier — `PrimarySource.tableName` / `pkColumn` / every entry
  in `hashedColumns`, plus `ChildSource.tableName` / `fkColumn` / every entry
  in `hashedColumns` — MUST match the regex
  `^[A-Za-z_][A-Za-z0-9_]{0,63}$`. Schema-qualified names (`db.tbl`),
  back-tick-quoted names, dots, hyphens, spaces, and any whitespace are
  REJECTED. Validation runs once at `CdcEngine.start()`; a single invalid
  identifier transitions the engine to `STATE_FAILED` and prevents tasks
  from being scheduled. Rationale: the engine interpolates these
  identifiers directly into Phase 1 / Phase 2 SQL (`CRC32(CONCAT_WS(',',
  <hashedColumns>))`, `BETWEEN ? AND ?` against `<pkColumn>`, etc.) —
  parameter binding only covers literal values. Treat this as a contract
  for `DbSchemaProvider` authors: declare bare identifiers, never quote or
  qualify.

- [wip] R20. **Multi-source entity assembly.** An `EntityMapping<T>` declares one
  `PrimarySource` and zero-or-more `ChildSource`s; each source is a separate
  SQL statement. The engine never emits cross-source `JOIN`s. Per child source:
    - Phase 1 SQL: `SELECT <fkColumn>, BIT_XOR(CRC32(CONCAT_WS(',', col1, col2,
      ...))) FROM <child.tableName> WHERE <fkColumn> BETWEEN ? AND ? GROUP BY
      <fkColumn>`.
    - Phase 2 SQL: `SELECT * FROM <child.tableName> WHERE <fkColumn> IN (?, ?,
      ...)` — same chunked-IN strategy as primary fetch.
    - Each child's Phase-1 contribution is XOR-folded into the per-PK aggregate
      CRC; orphan FKs (no matching primary row) are dropped.
    - Each child's Phase-2 rows are grouped by FK and passed to
      `mapping.mapEntity(primaryRow, childRowsByTable)` keyed by `tableName()`.
    - `BIT_XOR` collision risk: two child rows with identical CRC32 inside the
      same FK group cancel out in XOR. Per-child-row collision probability is
      `1/2^32`; for an entity with N child rows the per-cycle change-miss
      probability is bounded by `N(N-1)/2 × 1/2^32` (pairwise CRC collision)
      and is acceptable for game-data eventual consistency. Mitigation
      (`XOR COUNT(*)` row-count guard) is deferred — see Decisions.

- [done] R17. **Per-entity Kafka topic resolution from `ConnectResponse`.** Topic
  names are NOT constructed by the engine and are NOT declared by the schema provider
  — they arrive from the platform during the bootstrap handshake (see
  [`adapter-bootstrap` R16](../001-adapter-bootstrap.md), `ConnectResponse.syncTopics:
  Map<entityName, topic>`) and are passed into the engine via a `TopicResolver` SAM
  injected by `DbSyncModule`:
    - `String resolveTopic(String entityName)` — returns the platform-supplied topic
      name (e.g. `"bohpts.gs.sync.clans"` for `entityName = "clan"`), or `null` if the
      platform did not deliver a topic for this entity.
    - For each `EntityMapping`, the engine resolves the topic ONCE at engine start
      (snapshot from the connect-time map; no re-resolution per cycle).
    - **Per-entity behavior on missing topic:** if `resolveTopic(mapping.entityName())`
      returns `null` for a specific mapping, the engine logs an actionable WARN, marks
      that entity `DEGRADED` for every cycle (no Kafka publishes attempted), and the
      heartbeat surfaces it accordingly. Other entities keep running. Reload requires
      adapter restart with an updated `ConnectResponse`.
    - **Module-level behavior on empty / null `syncTopics` map:** handled in
      `DbSyncModule.onConnect` (see [`db-sync` spec](../003-db-sync/spec.md)) — module
      transitions to `DISABLED` with an actionable WARN. The defensive path is not
      expected to fire in production (the platform always delivers a non-empty map for
      tenants with sync enabled).

**Should:**

- [done] R12. The engine SHOULD send `SyncEvent`s with idempotent producer semantics —
  `enable.idempotence=true` configured in `nx-gs-kafka` defaults. Wire shape per row:
    - **Key:** `long` PK — serialized via Kafka `LongSerializer` (8 bytes,
      big-endian). Same row → same partition → ordering guarantee per row. Topic name
      itself encodes the (tenant, entity) tuple (delivered via
      `ConnectResponse.syncTopics`), so the key needs only the row identifier.
    - **Value:** Gson-serialized typed `SyncEvent<T> { entityName, pk: long, op:
      CREATED|UPDATED|DELETED, payload: T|null, timestamp }`. PK is a JSON number,
      not a string. `payload` is a JSON object (Gson serializes the typed slot
      directly), not an escaped string — platform consumer parameterizes its
      `Consumer<SyncEvent<ClanDbDto>>` against the same api artifact.
    - **DELETE wire shape:** `SyncEvent { op: DELETED, payload: null }` — Kafka
      value is a non-null JSON envelope (entityName + pk + op + null payload +
      timestampEpochMs) so the consumer keeps full audit (entity identity, op,
      timestamp). Topics in this slice run with bounded retention (≤1 day),
      not log compaction, so the value-null Kafka-tombstone optimization is
      intentionally not used — keeping the envelope is more useful for
      consumers that need to react to deletions and see when they happened.

- [wip] R13. The engine SHOULD NOT block waiting for Kafka acks per row. `NxKafka.send`
  returns immediately; the engine moves to the next row/window. A per-window
  `Long2ObjectMap<Future<RecordMetadata>>` keyed by PK tracks every in-flight send.
  At end-of-window the engine walks the map with a flush budget
  (`l2nx.cdc-engine.publish-flush-seconds`, default 5s) in TWO passes:
    1. First pass — iterate every in-flight future, call `Future.isDone()`
       (non-blocking). Done-and-acked futures classify as success and the
       per-PK snapshot advance happens immediately; done-and-failed futures
       are recorded for replay next cycle.
    2. Second pass — for futures still pending, wait with the remaining
       portion of the shared deadline via `f.get(remainingNs,
       NANOSECONDS)`. Each pending future blocks at most until the budget
       expires.

  Two passes avoid head-of-line blocking: previously, one slow ack at the
  head of the queue could starve later already-acked publishes against the
  shared budget. The done-first pass drains the easy outcomes immediately,
  the deadline-bounded pass cleans up the rest.

  Per-row outcome: succeeded → advance `SnapshotStore` for that PK (R1 last
  bullet); failed / timed-out → leave snapshot untouched, next cycle's
  Phase-1 diff replays the publish. No retry logic in the engine itself;
  producer-side retries are nx-gs-kafka's concern.

**Could:**

- [todo] R14. The engine COULD support dynamic per-entity parameter overrides
  (tickInterval, rowsPerWindow, queryTimeout, etc.) delivered via the platform —
  `nexus.adapter.sync-config` Kafka topic with key = adapter type. This requires a new
  INBOUND Kafka consumer in the adapter (currently fully outbound) — a significant
  architectural shift. MVP relies on R15 global override + provider declarations
  (static, restart to change); the config topic feature is deferred until a real ops
  case demands per-entity dynamic tuning.

- [done] R18. **Persisted snapshot cache.** The in-memory snapshot (R4)
  is periodically dumped to disk so a host-JVM restart does not trigger a
  full initial-sync replay (R7), and — more critically — so DELETE events
  fire for rows removed from the host DB while the adapter was offline
  (otherwise the next cycle's diff against an empty snapshot classifies
  everything as CREATE and orphan rows linger forever in `nx-gameservers`).
  Implemented as the [`snapshot-persistence`](../012-snapshot-persistence.md)
  feature — see that spec for requirements, edge cases, and the file
  format. Operationally: two knobs in the same `l2nx.cdc-engine.*`
  namespace — `persist.dir` (default `nx-cdc-snapshot`) and
  `persist.checkpoint-min-interval-seconds` (default 300s). Feature is
  always on; there is no disable flag.

**Non-goals:**

- **Cross-source SQL JOINs.** Each source (primary + each child) is one isolated
  SQL statement bounded by its own `BETWEEN ? AND ?` (or `IN (...)`) clause; the
  engine never composes a JOIN across primary + child. JOIN-based hashing would
  couple sources to a tenant-specific schema layout (column aliases, ON
  conditions) and prevent the per-source SPI from being agnostic. Aggregation
  happens in-engine via XOR.
- **Orphan child rows surface as entities.** Child rows whose FK has no matching
  primary row are silently dropped from the aggregate CRC and never reach
  `mapEntity`. The entity does not exist without a primary row. Cleanup of
  orphans is the host DB's responsibility, not the adapter's.
- **1 source table → N entities (fan-out).** The current SPI is N tables → 1
  entity (each `EntityMapping` collapses 1 primary + K children into one DTO).
  The reverse direction (one shared source table feeding multiple entities)
  would require a different SPI shape and is out of scope until a real customer
  ships it.
- **Cross-phase transactional consistency** — Phase 1 and Phase 2 run in SEPARATE
  consistent-snapshot transactions. Between them, a row may be deleted or further
  updated. The engine treats Phase-2 absence of a previously-detected PK as "no work for
  this row" and detects the deletion on the next cycle. Wrapping the whole cycle in one
  REPEATABLE READ would hold a multi-minute transaction on the host DB —
  operator-hostile.
- **Topic creation by the adapter** — the engine publishes to `gs.sync.*` topics; topic
  provisioning (with `cleanup.policy=compact`, retention, partitioning) is the
  platform's responsibility. Adapter does NOT instantiate `AdminClient` or attempt to
  create topics on first publish.
- **Multiple sync strategies (`FULL_SCAN` / `SLIDING_WINDOW` / etc.)** — engine ships
  ONE strategy: PK-windowed scan with `rows-per-window` partition (R2). Small entities
  collapse to a 1-window cycle = effectively full scan, no separate code path needed.
  `SyncStrategy` enum and `EntityMapping.strategy()` / `EntityMapping.windowCount()`
  fields are explicitly NOT in the API.
- **Cycle ordering by row count** — engine processes mappings in the order returned
  by `provider.mappings()`. Engine does NOT sort by observed row count or do
  "small-entities-first" reordering. Provider authors arrange the list manually.
- **Real-time durability of the freshest mutations** — the snapshot
  cache (R18) writes per-entity at most every
  `persist.checkpoint-min-interval-seconds` (default 5 min). A crash within
  that window loses the last few cycles' advance; the next cycle's Phase-1
  diff re-detects via the live DB scan, not via a WAL replay. Not a real-
  time durability ledger.
- **Hash function alternatives** — CRC32 is hardcoded. CRC64 / xxHash / SHA would expand
  the hash domain at the cost of doubled RAM (R4) and require client-side computation
  (defeating Phase 1's "MySQL computes hashes server-side" advantage). See Decisions in
  the Technical design section below for collision-risk rationale.
- **Per-entity operator overrides** — `l2nx.cdc-engine.<param>.<entityName>`-style
  keys are NOT in MVP. The R15 chain accepts only ONE global key per parameter
  (`l2nx.cdc-engine.tick-interval-seconds`, `l2nx.cdc-engine.rows-per-window`) which
  applies uniformly across all entities. Per-entity granularity (e.g. clans every 60s
  but items every 600s with operator-controlled overrides) arrives via R14's dynamic
  Kafka config when a real ops case demands it.
- **Per-entity query timeout overrides** — global
  `l2nx.cdc-engine.query-timeout-seconds` only. Per-entity tunability is YAGNI for
  MVP; revisit when a real workload pattern demands it.
- **Composite-PK and non-numeric-PK entities** — engine assumes numeric PK readable as
  `long` (Tier-2 SPI Non-goal in `db-sync`). Wire shape commits to `long` PK both in
  Kafka key (`LongSerializer`) and `SyncEvent.pk`. Adding String-PK or composite-PK
  support requires a parallel `Object2IntOpenHashMap<String>` snapshot path AND a
  wire-shape variant — deferred until a real customer ships such a schema.
- **Per-entity heartbeat stats topics** — per-entity operational state is surfaced
  via `HeartbeatEvent.enabledModules[*].stats.entities[]` (R10), NOT separate
  `gs.sync.{entity}.stats` Kafka topics. Single channel; existing platform-side
  heartbeat consumer reads everything.
- **Inbound Kafka consumer for dynamic config (R14)** — out of scope for MVP. Static
  `tickInterval` only; reload requires adapter restart.
- **Engine-side retry on Kafka send failure** — single attempt per cycle. Producer-side
  retries (within nx-gs-kafka producer config) handle transient broker hiccups.
  Persistent failures abort the cycle's snapshot swap; the next tick replays the diff.

## Open questions

- [assumed: Default `l2nx.cdc-engine.query-timeout-seconds = 10`. Rationale: 12M-row Phase 1
  scan typically completes in 20–40s, so individual chunks should not approach 10s; a
  10s ceiling catches stalls without crippling normal operation. Tunable per operator.]
- [assumed: Default `l2nx.cdc-engine.rows-per-window = 500_000`. Yields 1 window for
  small entities (clans 1k, characters 152k) and ~24 windows for items (12M). Tunable
  per operator. No per-entity override in MVP.]
- [assumed: Default `l2nx.cdc-engine.publish-flush-seconds = 5`. End-of-cycle wait for all
  in-flight Kafka sends to ack. If exceeded, cycle marked DEGRADED and snapshot not
  advanced. Tunable per operator.]
- [resolved: SyncEvent is **typed** (`SyncEvent<T>`). Gson serializes the payload slot
  as a JSON object (not an escaped string). Platform-side consumer compiles against
  the same `nx-gs-adapter-api` artifact and parameterizes its consumer per topic
  (`Consumer<SyncEvent<ClanDbDto>>` for `bohpts.gs.sync.clans`). Adding a new entity
  bumps the api minor; coordinated upgrade with the platform.]
- [resolved: Snapshot swap is **per-row**. Engine tracks a per-cycle
  `Long2ObjectMap<Future<RecordMetadata>>` keyed by PK; at end-of-cycle (within
  `publish-flush-seconds`) the engine walks the map. For each successfully-acked
  PK: advance its CRC32 in `SnapshotStore` (created/updated → put new CRC; deleted →
  remove). For failed/timed-out PKs: leave snapshot untouched — next cycle's Phase-1
  diff will re-detect them and replay the publish. Rationale: a 12M-row replay
  triggered by a single flaky send is unacceptable; per-row recovery cost is bounded
  by the number of in-flight Future references per cycle (~thousands at most).
  All-or-nothing was rejected.]
- [resolved: Phase-2 missing rows (PKs requested but not returned because the row
  was deleted between Phase 1 and Phase 2) are a **silent no-op** for the current
  cycle — no fabricated `DELETED` event. The next cycle's Phase 1 detects the PK as
  absent and emits the deletion tombstone. Trade-off: delete latency = up to one
  `tick-interval` in the missing-row case (60s default). Acceptable for game data
  (no real-time SLA on clan/character/item state). Same-cycle fabrication would
  require comparing requested vs returned PK lists and synthesizing a payload-less
  event; the next-cycle path uses the existing diff machinery and is simpler.]
- [resolved: **Multi-source entity assembly via PrimarySource + ChildSource.** Each
  `EntityMapping<T>` declares one `PrimarySource` (drives windowing + identity) and
  zero-or-more `ChildSource`s (each with FK back to primary's PK). DTO is built by
  `T mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable)` —
  single hook, no per-shape (ONE/MANY/KV) enum. Per-source rows are opaque
  `Object`s in the engine; impl casts inside `mapEntity` to its private row
  types. Fan-out (1 source → N entities) explicitly out of scope.]
- [resolved: **Phase-1 child hashing via `BIT_XOR(CRC32(...))` aggregate.** Per
  child source: `SELECT fk, BIT_XOR(CRC32(CONCAT_WS(',', cols))) FROM child WHERE
  fk BETWEEN ? AND ? GROUP BY fk`. Per entity PK: `entityCrc = primaryCrc XOR
  child1Crc XOR child2Crc XOR ...`. Order-insensitive; child row order has no
  semantic meaning. Collision risk for two child rows with identical CRC32 inside
  the same FK group is `~1/2^32` per pair — acceptable for eventual-consistency
  game data; row-count guard (`XOR COUNT(*)`) is documented but deferred until
  a real collision surfaces. Engine drops orphan child FKs (no matching primary
  row) silently.]
- [resolved: **DELETE detection via snapshot envelope.** `WindowPlanner` partitions
  `[min(MIN_db, MIN_snapshot), max(MAX_db, MAX_snapshot)]` instead of `[MIN_db,
  MAX_db]`. Closes the boundary bug where deleting the row at current
  `MIN(pk)` / `MAX(pk)` shrinks the DB range, leaves the deleted PK outside every
  next-cycle window, and never emits its tombstone. Open-hash-backed
  `SnapshotStore` exposes `minPk(entity)` / `maxPk(entity)` via O(N)
  full-key scans called once per cycle (envelope planning) — at 6.5M items
  ~50 ms, dwarfed by the JDBC `SELECT MIN(pk), MAX(pk)` it brackets.]
- [resolved: `windowCount()` does NOT live on `EntityMapping`. Engine uses a
  single global `l2nx.cdc-engine.rows-per-window` knob (R15) to derive window
  count per-cycle from `MIN/MAX(pk)`; R3's `SLIDING_WINDOW`-as-a-mode was
  folded into R2 along with `mapping.strategy()` / `mapping.windowCount()`.
  Tier-2 SPI in `db-sync` R5 carries no engine-config fields.]
- [resolved: `SnapshotStore` is backed by `Long2IntOpenHashMap`, NOT
  `Long2IntAVLTreeMap`. Earlier the AVL tree was chosen for O(log N + k)
  `keysInRange` and O(log N) `minPk` / `maxPk`. At a tenant with 6.5M items
  on a 4 GB heap this cost ~360 MB resident on snapshot alone — combined
  with cycle-resident `inFlight` accumulators (pre-fix, see R1 above) it
  pushed the host JVM into OOM. Switched to `Long2IntOpenHashMap`: ~16 B
  per entry (~100 MB at 6.5M). Per-cycle `keysInRange` work is now O(N
  log W) via top-of-cycle bucketing by window (binary search on plan
  boundaries); `minPk` / `maxPk` are tracked incrementally on every
  `putCrc` / `removeCrc` with lazy memoization. Total per-cycle iteration
  drops from ~288M (naive O(N) × W) to ~N at items scale. Default-return
  sentinel `Integer.MIN_VALUE` exposed as `MISSING_HASH` removes the
  hash=0 / "absent key" ambiguity at the type level. The earlier
  "open-hash goes quadratic at items scale" concern was scoped to the
  cycle-level flush design — once per-window flush bounds the in-flight
  window to `rowsPerWindow`, total per-cycle work stays at O(N log W).
  R4's wording above is updated accordingly.]
- [resolved: `WindowPlanner.MAX_WINDOWS_PER_PLAN = 1_000_000` sanity cap on
  the planning step plus the new `rowsPerWindow ≤ 10_000_000` engine-config
  cap together protect the host JVM from pathological PK ranges and
  operator misconfiguration. Plan size > cap throws `IllegalStateException`
  (engine catches and marks the entity DEGRADED). Not promoted to a
  numbered Must — it's an internal safety net on top of the operator-
  visible `rows-per-window` knob.]
- [NEEDS CLARIFICATION: R15 says config is "read via `ConfigResolver` from
  `adapter-bootstrap`", but `:nx-gs-db-sync-core` cannot depend on
  `:nx-gs-adapter-core` (api-only contract). Code instead has a tiny
  `EngineConfig.productionChain()` replica with the same file-first /
  sysprop-fallback shape. Functionally equivalent; should R15 be amended to
  "same source chain as `ConfigResolver`" rather than literally use the class?
  ref: `nx-gs-db-sync-core/.../engine/EngineConfig.java:88-100`]
- [NEEDS CLARIFICATION: R1 Phase 2 spec says
  `SELECT <fetchColumns> FROM <table> WHERE <pk> IN (...)`, but
  `EntityMapping` has no `fetchColumns()` method — the SPI was deliberately
  trimmed to `entityName / tableName / pkColumn / hashedColumns / mapRow /
  dtoType`. Code uses `SELECT *` and lets `mapRow(rs)` pull only the columns
  it needs. Should R1 be amended to `SELECT *` (matches code), or should
  `fetchColumns()` be added to the SPI for narrower transfer over the wire?
  ref: `nx-gs-db-sync-core/.../engine/phase/Phase2Fetcher.java:90`]

## Links

- Sibling feature (Tier-1 SPI + heartbeat types):
  [`docs/specs/002-adapter-modules/spec.md`](../002-adapter-modules/spec.md) — defines
  `ModuleStatus` / `Stats` / `EntityStats` / `EntityState` / `ChangesSummary` consumed by
  R10
- Sibling feature (Tier-2 SPI + module wiring):
  [`docs/specs/003-db-sync/spec.md`](../003-db-sync/spec.md) — provides `DbSchemaProvider` /
  `EntityMapping` shape; this engine consumes them
- Sibling feature (Tier-3 SPI):
  [`docs/specs/004-jdbc-connection-source.md`](../004-jdbc-connection-source.md) —
  engine borrows `Connection`s through the SPI; read-only enforced via SQL
  `START TRANSACTION ... READ ONLY`, not via `Connection.setReadOnly`
- fastutil project: https://fastutil.di.unimi.it/ — `it.unimi.dsi:fastutil-core`
  artifact (~3 MB primitive-maps subset)

---

## Technical design

> Covers: spec.md
> Sibling: [`db-sync spec`](../003-db-sync/spec.md) — module wiring + Tier-2 SPI shape

### Overview

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

### Structure

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
          MySQL Connector/J (`jdbc:mysql:`, detected via `JdbcDialect.detect`)
          gets `Integer.MIN_VALUE` for row-by-row streaming (the only mode it
          honors for large result sets); MariaDB Connector/J (`jdbc:mariadb:`),
          Postgres, and other drivers get `cfg.fetchSize` (default `10_000`)
          as a server-side cursor batch hint (MariaDB 3.x rejects negative
          fetchSize; add `useCursorFetch=true` to the URL for true streaming). Each statement is
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

### Key components

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

### Data flows

#### 0. Engine startup — config resolution + log

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
       (NB: MySQL Connector/J ignores positive fetchSize; the engine auto-detects
        dialect via JdbcDialect.detect at first borrow and switches to
        Integer.MIN_VALUE streaming on `jdbc:mysql:`. MariaDB Connector/J 3.x
        rejects negative fetchSize, so `jdbc:mariadb:` keeps the positive hint
        — add `useCursorFetch=true` to the URL for true server-side cursors.)
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

#### 1. Cycle (multi-source windowed strategy)

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

#### 2. Heartbeat enrichment

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

### Decisions

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
  `applyFetchSize` accordingly: MySQL Connector/J (`jdbc:mysql:`) →
  `Integer.MIN_VALUE` (row-by-row streaming, the only mode it honors
  for large result sets — positive fetchSize is silently ignored);
  MariaDB Connector/J (`jdbc:mariadb:`), Postgres, and other drivers →
  `cfg.fetchSize` as a server-side cursor batch on `autoCommit=false`.
  MariaDB Connector/J 3.x is split out because it validates
  `fetchSize >= 0` and throws `SQLException: invalid fetch size` on the
  MySQL streaming sentinel; for true streaming add `useCursorFetch=true`
  to the JDBC URL. Without dialect-aware routing, a 12M-row Phase 1 scan
  on a MySQL host buffers every row into the client heap and OOMs the
  JVM before the snapshot map fills.
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
- **Persisted previous-snapshot cache (R18) — delivered as
  [`snapshot-persistence`](../012-snapshot-persistence.md).** Per-entity
  binary file `<persist-dir>/<schema>/<entityName>.snap` (magic + version +
  name + count + dense `(long pk, int crc32)` pairs + trailing CRC32),
  written via tmp → fsync → atomic rename, throttled at
  `persist.checkpoint-min-interval-seconds` (default 300s), force-flushed
  on shutdown. Directory lock via `FileChannel.tryLock` on `.lock` prevents
  two adapter JVMs on the same host from racing. Loaded once on
  `CdcEngine.start` BEFORE the first tick is scheduled — closes the
  orphan-on-restart bug where rows deleted from the host DB during
  adapter downtime were never observed (next cycle's diff against empty
  snapshot would classify everything as CREATE, never DELETE). Wired by
  `DbSyncModule.buildPersistence`; lock-acquisition failure transitions
  the module to `STATE_FAILED`. Schema-fingerprint invalidation deferred —
  the simpler `FORMAT_VERSION` bump-and-drop policy covers the only
  near-term invalidation case (engine-side wire format change); column-set
  / DTO-shape changes that don't bump `FORMAT_VERSION` are caught by
  natural re-CRC of the next cycle (the changed hash flows as an UPDATE
  event, not as a stale snapshot read).

### Extension points

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
