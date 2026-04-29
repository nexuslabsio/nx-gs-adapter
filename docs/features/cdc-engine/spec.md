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
    > `JdbcConnectionSource` — the engine borrows connections through it; per-borrow
    > `setReadOnly(true)`.

**Must:**

- [todo] R1. The engine MUST execute the CRC32 two-phase protocol per `EntityMapping`
  on every scheduled tick. The protocol always runs in PK-windowed mode (R2) — there
  is no separate "full scan" code path; small entities collapse to a single window
  naturally:
    - **Phase 1 (detect):** `SELECT <pk>, CRC32(CONCAT_WS(',', col1, col2, ...)) FROM
      <table> WHERE <pk> BETWEEN ? AND ?` — MySQL computes hashes server-side; adapter
      reads PK as `long` and CRC32 as `int` into a fastutil `Long2IntOpenHashMap`. The
      `WHERE <pk> BETWEEN ? AND ?` clause is always present, bounded by the current
      window's `[fromPk, toPk]` (R2).
    - **Diff:** previous-snapshot vs current-snapshot for the window's PK range →
      `{ created: LongSet, updated: LongSet, deleted: LongSet }`. Created = present in
      current, absent in previous; deleted = inverse; updated = present in both with
      different CRC32. Only PKs in `[fromPk, toPk]` are evaluated — PKs outside the
      window stay untouched in the persistent snapshot.
    - **Phase 2 (fetch):** `SELECT <fetchColumns> FROM <table> WHERE <pk> IN (?, ?,
      ...)` for `created ∪ updated`, chunked at 1000 PKs per query. PKs bound via
      `setLong(...)`. Rows mapped via `mapping.mapRow(rs)`.
    - **Publish:** `(mapping, op, pk, dto|null)` translated to a `SyncEvent` and pushed
      via the Kafka producer initialized by `adapter-bootstrap`. PK is `long`
      end-to-end — engine internals (fastutil maps, JDBC `setLong`/`getLong`), wire
      payload (`SyncEvent.pk: long`, JSON number on the wire), and Kafka key (binary
      `LongSerializer`, 8 bytes) all carry the raw long. No stringification anywhere —
      platform stores the PK as `long` and reads it back as `long`. Topic name comes
      from `ConnectResponse.syncTopics[entityName]` (per
      [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)).
    - **Per-row snapshot swap:** the in-memory `Long2IntOpenHashMap` for the entity
      is advanced **per PK**, not per window. Each Phase-2 publish records its
      `Future<RecordMetadata>` keyed by PK in a per-cycle in-flight map. At
      end-of-cycle (within `publish-flush-seconds`) the engine walks the map and
      advances `SnapshotStore` only for PKs whose publish succeeded
      (created/updated → put new CRC32 from current scan; deleted → remove). PKs
      whose publish failed (or timed out) are left untouched in the previous
      snapshot, so the next cycle's diff re-detects them and replays the publish.
      PKs outside the current window's `[fromPk, toPk]` are never touched.

- [todo] R2. **Single windowed sync strategy with `rows-per-window` config.** There is
  one strategy: PK-range windowed scan. The engine partitions the entity's PK range
  into windows and walks them sequentially within one cycle, back-to-back, no pause:
    - At the start of every cycle the engine runs `SELECT MIN(<pk>), MAX(<pk>) FROM
      <table>` (recompute, NOT cached at connect — auto-extends to capture inserts
      above prior MAX).
    - PK range size = `MAX - MIN + 1`. Window count = `max(1, ceil(pkRange /
      rowsPerWindow))`. Window boundaries = even subdivision of `[MIN, MAX]` into that
      many half-open intervals.
    - `rowsPerWindow` comes from `l2nx.cdc-engine.rows-per-window` (default 500_000)
      per R15. One global value applied uniformly to every entity.
    - Small entities (rowCount <= rowsPerWindow): yield exactly 1 window covering the
      entire PK range — operationally identical to a "full scan" without a separate
      code path.
    - Large entities (e.g. 12M items, 500k rows-per-window default → 24 windows):
      sequential walk through every window inside one cycle.
    - Sparse-PK entities (auto-increment + many deletions, large gaps in PK range):
      window count over-estimates relative to actual row count — windows simply contain
      fewer rows than `rowsPerWindow`. Operationally fine; predictable.
    - The single in-memory `Long2IntOpenHashMap` per entity covers all windows of that
      entity (per R4) — only the window currently being scanned is touched per
      iteration.
    - SC1. MIN/MAX recompute query completes in < 50ms even on the largest target
      entity (~12M items) on a host with PK index intact.

> **R3 (formerly `SLIDING_WINDOW` strategy as a separate mode) — folded into R2.**
> Single-strategy decision: removed the `SyncStrategy { FULL_SCAN | SLIDING_WINDOW }`
> enum and `EntityMapping.strategy()` / `EntityMapping.windowCount()` fields. The
> "small entity = 1 window, large entity = N windows" semantics fall out of R2's
> rowsPerWindow math without a per-entity strategy switch. Number R3 is intentionally
> left as a gap.

- [todo] R4. The engine MUST hold each entity's previous-snapshot in a fastutil
  `it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap` (PK → CRC32). One map per
  `EntityMapping`, lifetime = adapter lifetime. Wiped on `DbSyncModule.onDisconnect`. RAM
  cost: ~24 bytes/entry → ~240 MB at 12M entries with default load factor 0.75.
  `fastutil-core` (~3 MB JAR; primitive maps only — full `fastutil` ~21 MB is NOT pulled)
  is added as a runtime dep on `nx-gs-db-sync-core`.
    - SC2. For 12M entries, RAM occupancy of one snapshot stays under 320 MB measured via
      `Runtime.totalMemory() - Runtime.freeMemory()` delta around the snapshot population.

- [todo] R5. The engine MUST run each `EntityMapping` on a daemon
  `ScheduledExecutorService` with **one thread per entity**:
    - Thread name: `nx-cdc-{schemaName}-{entityName}`
    - First tick fires immediately after `DbSyncModule.start` (initial sync — see R7);
      subsequent ticks at the engine-global tick interval from
      `l2nx.cdc-engine.tick-interval-seconds` (R15) — every entity ticks at the same
      cadence
    - Tick wrapped in `SafeRunnable` (reused from adapter-bootstrap) so an uncaught
      throwable does not cancel the schedule

- [todo] R6. **Cycle order — provider list.** When `DbSyncModule` configures the
  engine with `provider.mappings()`, the engine launches one scheduler thread per
  entity in the ORDER returned by the provider. Within a single thread, ticks happen
  at the mapping's `tickInterval()`; entities on different threads run independently
  and can overlap. Provider authors are expected to declare small/fast entities first
  if cross-entity cycle ordering matters (informational guideline — engine does not
  enforce or sort by row count).

- [todo] R7. **Initial sync** — first tick after `DbSyncModule.start()` MUST replay
  every existing row as `CREATED` events. Previous snapshot is empty → diff for each
  window returns all PKs in that window as created → Phase 2 fetches all rows →
  publishes one `SyncEvent { op: CREATED }` per row. No special bootstrap mode; the
  engine's normal windowed Phase 1 + Phase 2 path handles initial sync naturally —
  every window's PKs come back as created on the first cycle, then nothing on
  subsequent cycles unless data actually changes.

> **R8 (per-entity snapshot RAM cap) — removed.** The engine does NOT enforce a
> snapshot row-count cap. Operators size the host JVM heap to fit the configured
> entities; an entity that grows beyond expected size simply uses more RAM. Number
> R8 left as a gap. `EntityState.SKIPPED` is therefore also removed (its sole
> producer was the cap path) — `EntityState` enum carries only `HEALTHY | DEGRADED`.

- [todo] R9. The engine MUST apply a global query timeout via
  `Statement.setQueryTimeout(int seconds)` to every Phase 1 and Phase 2 query. Default
  10s, configurable via `l2nx.cdc-engine.query-timeout-seconds`. On
  `SQLTimeoutException`, the current window is aborted, the per-entity state
  transitions to `DEGRADED` for that cycle, the engine continues to the next window
  (or, if it was the last window, the next tick), and the snapshot for the affected
  window is NOT advanced.

- [todo] R10. The engine MUST publish per-entity operational state on every cycle into
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

- [todo] R11. **Per-query InnoDB consistent snapshot** — every Phase 1 query (full scan
  or window) and every Phase 2 chunk MUST execute inside its own transaction opened with
  `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` (or equivalent
  `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` + first SELECT semantics on
  MariaDB / MySQL). Transaction is committed (no rollback path needed) immediately
  after the result set is drained. Rationale: Phase 1's scan over a 12M-row table can
  take 20–40s; without a consistent snapshot, the scan mixes pre/post-update versions
  of rows and produces false-positive diffs.

- [todo] R15. **Engine config from `l2nx.properties` only — no provider-side
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

  | Key                                          | Type           | Default   |
            |----------------------------------------------|----------------|-----------|
  | `l2nx.cdc-engine.tick-interval-seconds`      | long, seconds  | 60        |
  | `l2nx.cdc-engine.rows-per-window`            | int            | 500_000   |
  | `l2nx.cdc-engine.query-timeout-seconds`      | int, seconds   | 10        |
  | `l2nx.cdc-engine.publish-flush-seconds`      | int, seconds   | 5         |

  Every entity ticks at the same `tick-interval-seconds`, every entity uses the
  same `rows-per-window` partition size, every Phase 1 / Phase 2 query uses the
  same `query-timeout-seconds`. No per-entity overrides, no provider-supplied
  defaults.

  **Resolution is one-shot at engine start** — values are cached as `EngineConfig`
  for the lifetime of the engine. Operator changes to `l2nx.properties` require
  adapter restart. Dynamic per-entity reload is post-MVP via R14's inbound Kafka
  consumer.

- [todo] R16. **Rich startup log** — at `CdcEngine.start()`, after R15 resolution
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

- [todo] R17. **Per-entity Kafka topic resolution from `ConnectResponse`.** Topic
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

- [todo] R12. The engine SHOULD send `SyncEvent`s with idempotent producer semantics —
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
    - **Tombstone for delete:** `SyncEvent { op: DELETED, payload: null }` — value is a
      non-null JSON envelope but the payload-slot is null. Compaction-friendly because
      the key uniquely identifies the row.

- [todo] R13. The engine SHOULD NOT block waiting for Kafka acks per row. `NxKafka.send`
  returns immediately; the engine moves to the next row/window. A per-cycle
  `Long2ObjectMap<Future<RecordMetadata>>` keyed by PK tracks every in-flight send.
  At the end of a cycle the engine walks the map with a short flush budget
  (`l2nx.cdc-engine.publish-flush-seconds`, default 5s). Per-row outcome:
  succeeded → advance `SnapshotStore` for that PK (R1 last bullet); failed /
  timed-out → leave snapshot untouched, next cycle's Phase-1 diff replays the
  publish. No retry logic in the engine itself; producer-side retries are
  nx-gs-kafka's concern.

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
- [NEEDS CLARIFICATION: `windowCount()` lives on `EntityMapping` (Tier-2 SPI in `db-sync`)
  with a default method returning 10. Confirm that the SPI shape exposes this — currently
  `db-sync` R5 mentions it implicitly under `SLIDING_WINDOW` strategy but doesn't list
  `windowCount` as a method. Cross-spec sync needed once cdc-engine R3 is finalized.]

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
  engine borrows `Connection`s through the SPI; per-borrow `setReadOnly(true)`
- fastutil project: https://fastutil.di.unimi.it/ — `it.unimi.dsi:fastutil-core`
  artifact (~3 MB primitive-maps subset)
