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
> - [`db-sync`](../db-sync/spec.md) provides `EntityMapping` / `DbSchemaProvider` (Tier-2
    > SPI shape) — the engine accepts these uniformly, no engine code knows about specific
    > entities.
> - [`adapter-modules`](../adapter-modules/spec.md) defines `ModuleStatus` / `Stats` /
    > `PoolStats` / `EntityStats` / `EntityState` / `ChangesSummary` types — the engine
    > populates `Stats.entities[]` per cycle.
> - [`jdbc-connection-source`](../jdbc-connection-source/spec.md) provides
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
      [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)).
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
  [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md), `ConnectResponse.syncTopics:
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
      `DbSyncModule.onConnect` (see [`db-sync` spec](../db-sync/spec.md)) — module
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
      `Consumer<SyncEvent<ClanDto>>` against the same api artifact.
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

- [todo] R18. **Persisted snapshot cache (post-MVP).** The in-memory snapshot
  (R4) could be periodically dumped to disk so that a host-JVM restart does not
  trigger a full initial-sync replay (R7) for every entity. On engine start, the
  cache would be loaded if present, validated against the current schema/CRC32 of
  hashed columns (mismatch → discard and full resync), then merged into the active
  snapshot. Trade-offs to evaluate during the dedicated slice: hash invalidation
  policy on schema-provider version bump, cache file location vs container
  ephemeral filesystems, fsync cadence vs IO impact, corruption recovery, multi-JVM
  cohabitation on the same host. Out of MVP scope — see Decisions in tech.md for
  the analysis.

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
- **Persistent snapshot store (in MVP)** — MVP snapshots are heap-only and lost on
  JVM restart. Cold start replays every entity as initial sync (R7). A 12M-row
  initial sync is acceptable bursty cost on rare reboots. Persisting snapshots is
  tracked as R18 (post-MVP) — see Decisions in tech.md for the feasibility analysis
  (cache file format, schema-version invalidation, fsync cadence).
- **Hash function alternatives** — CRC32 is hardcoded. CRC64 / xxHash / SHA would expand
  the hash domain at the cost of doubled RAM (R4) and require client-side computation
  (defeating Phase 1's "MySQL computes hashes server-side" advantage). See Decisions in
  tech.md for collision-risk rationale.
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
  (`Consumer<SyncEvent<ClanDto>>` for `bohpts.gs.sync.clans`). Adding a new entity
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
  [`docs/features/adapter-modules/spec.md`](../adapter-modules/spec.md) — defines
  `ModuleStatus` / `Stats` / `EntityStats` / `EntityState` / `ChangesSummary` consumed by
  R10
- Sibling feature (Tier-2 SPI + module wiring):
  [`docs/features/db-sync/spec.md`](../db-sync/spec.md) — provides `DbSchemaProvider` /
  `EntityMapping` shape; this engine consumes them
- Sibling feature (Tier-3 SPI):
  [`docs/features/jdbc-connection-source/spec.md`](../jdbc-connection-source/spec.md) —
  engine borrows `Connection`s through the SPI; read-only enforced via SQL
  `START TRANSACTION ... READ ONLY`, not via `Connection.setReadOnly`
- fastutil project: https://fastutil.di.unimi.it/ — `it.unimi.dsi:fastutil-core`
  artifact (~3 MB primitive-maps subset)
