# Plan: Clan sync MVP (full end-to-end)

> **Covers (db-sync feature scope):** R3, R4, R5, R9 (Phase 2 part), R10, R11, R12 (Phase 2 part), R16
>
> **Cross-feature scope (delivered as part of this MVP):**
> - [`adapter-modules`](../../adapter-modules/spec.md) R2, R3, R8 — `ConnectContext.syncTopics`, `Stats.entities` slot +
    `EntityStats` / `EntityState` / `ChangesSummary` types, `PoolStats` rename, api/0.6.0 cut
> - [`adapter-bootstrap`](../../adapter-bootstrap/spec.md) R16 — `ConnectResponse.syncTopics`
> - [`jdbc-connection-source`](../../jdbc-connection-source/spec.md) R7 — bohpts `PoolStats` mapping to renamed shape
> - [`cdc-engine`](../../cdc-engine/spec.md) R1, R2, R4, R5, R6, R7, R9, R10, R11, R12, R13, R15, R16, R17 — entire
    MVP-track engine. R14 (dynamic Kafka config) and R18 (persisted snapshot) are explicitly out of scope; deferred.
>
> **Resolved decisions (lock into specs in M1):**
> - `SyncEvent<T>` is **typed**, parameterized by DTO class. Platform consumer compiles against `SyncEvent<ClanDbDto>`.
    Future-entity bumps the api artifact; coordinated upgrade is acceptable for the small entity catalog.
> - Snapshot swap is **per-row**. End-of-cycle the engine walks the per-PK `Future<RecordMetadata>` map, advances
    snapshot only for PKs whose publish succeeded; failed PKs stay in the previous snapshot and are replayed on the next
    cycle.
> - Phase-2 missing rows (deleted between phases) are a **silent no-op**; the next cycle's Phase-1 diff catches the
    deletion and emits the tombstone. No same-cycle fabricated `DELETED`.

## Approach

Drop `nx-gs-db-sync-core-0.1.0.jar` + `nx-gs-adapter-api-0.6.0.jar` into a host JVM that
ships `BohptsDbSchemaProvider` + `BohptsJdbcConnectionSource`, point the adapter at the
L2NX platform, and observe clan rows flowing into the platform-supplied Kafka topic. The
work splits into six layers, each one buildable independently against the previous:

1. **Wire shape** — `nx-gs-adapter-api` 0.6.0 carries the new types: `EntityStats`,
   `EntityState (HEALTHY|DEGRADED)`, `ChangesSummary`, renamed `PoolStats { active, idle,
   total, waiting }` (all `Integer` nullable), `SyncEvent<T>` (typed), `ClanDbDto` (long PK +
   primitive-for-NOT-NULL convention), `Map<String,String> syncTopics` on `ConnectResponse`
   and `ConnectContext`. Tier-2 SPI (`DbSchemaProvider`, `EntityMapping<T>`) lives in
   `:nx-gs-adapter-api` package `app.l2nx.gs.adapter.api.spi` (alongside Tier-1
   `AdapterModule` and Tier-3 `JdbcConnectionSource` — provider authors depend only
   on `nx-gs-adapter-api`, not on `nx-gs-db-sync-core`).
2. **Bootstrap plumbing** — `:nx-gs-adapter-core` parses `ConnectResponse.syncTopics`,
   threads it through `ConnectContext`. `:nx-gs-db-sync-core`'s `DbSyncModule.onConnect`
   consults `ctx.syncTopics()`; empty/null → DISABLED with actionable WARN.
3. **CDC engine** — entirely inside `:nx-gs-db-sync-core` package
   `app.l2nx.gs.db.sync.engine`: `EngineConfig` (l2nx.cdc-engine.* keys), `SnapshotStore`
   (one fastutil `Long2IntOpenHashMap` per entity), `WindowPlanner` (MIN/MAX recompute,
   ceil division), `Phase1Hasher` (CRC32 SELECT, diff produce LongSets),
   `Phase2Fetcher` (chunked IN(...) fetch, mapRow), `SyncEventPublisher` (long Kafka key
   via `LongSerializer`, Gson value, fire-and-forget with end-of-cycle per-row flush walk),
   `EntitySyncTask` (orchestrate one cycle), `EntityStatsTracker` (volatile per-cycle
   stats), `TopicResolver` SAM bound to `ctx.syncTopics()`, `ConfigResolutionLogger`
   (cdc-engine R16 startup log), `CdcEngine` top-level (one ScheduledExecutorService
   thread per entity, SafeRunnable wrap).
4. **DbSyncModule wiring** — `DbSyncModule.onConnect` reads `ctx.syncTopics()` (DISABLED
   on empty/null); resolves `DbSchemaProvider` via ServiceLoader (0 → DISABLED, 1 → use,
   > 1 → FAILED); instantiates `CdcEngine`. `currentStatus()` surfaces both `pool` and
   `entities` slots in the heartbeat. `stop` / `onDisconnect` tear down cleanly.
5. **Bohpts schema provider** — in bohpts-core repo (`E:/bohpts/code/bohpts-core`):
   `BohptsDbSchemaProvider` returning one `EntityMapping<ClanDbDto>` (entity `"clan"`,
   table `"clan_data"`, PK `"clan_id"`, hashed columns 4 plain cols, `mapRow` building
   ClanDbDto with `long clanId` + `int clanLevel` + 0-sentinel-to-null for
   `leaderId`/`allyId`); service descriptor.
6. **End-to-end smoke** — Testcontainers MySQL with `clan_data` fixture + Testcontainers
   Kafka + WireMock platform that returns `syncTopics["clan"]="bohpts.gs.sync.clans"`.
   Assertions: initial sync emits N CREATED events; row update emits 1 UPDATED on next
   cycle; row delete emits 1 DELETED tombstone on next cycle; heartbeat carries
   `entities[clan]=HEALTHY`. Manual smoke in bohpts-core dev confirms the same path.

## Milestones

### Wire shape — `nx-gs-adapter-api` 0.6.0 + Tier-2 SPI

1. [x] **Lock resolved decisions in specs.** Update Open questions in
   `db-sync/spec.md` and `cdc-engine/spec.md` to `[resolved: ...]` for typed
   `SyncEvent<T>`, per-row snapshot swap, missing-row silent no-op. Refine
   cdc-engine R1 (last bullet) + R12 + R13 wording from "all-or-nothing window
   swap" to "per-row swap walking end-of-cycle Future map". Fix
   `adapter-modules/spec.md:102` stale reference to `EntityState.SKIPPED` (R8 strip
   note removed it; enum carries only `HEALTHY | DEGRADED`).

2. [x] **Add `EntityState` + `ChangesSummary` + `EntityStats`** in
   `app.l2nx.gs.adapter.api.kafka.ops`. POJOs with hand-written builders.
   `EntityState { HEALTHY, DEGRADED }`. `ChangesSummary { long created, long updated,
   long deleted }`. `EntityStats { String name, EntityState state, Long rowCount, Long
   lastSyncEpochMs, Long lastCycleDurationMs, ChangesSummary lastCycleChanges, Integer
   consecutiveErrors }`.

3. [x] **Rename `PoolStats { busy }` → `{ active }` + add `waiting`.** Four fields
   `Integer` (nullable). Update all callers. Update bohpts ref impl in
   `BohptsJdbcConnectionSource.stats()`: `getBusyConnectionCount()` →
   `PoolStats.active`, `getIdleConnectionCount()` → `.idle`, leave `total` /
   `waiting` null (bohpts `DatabaseFactory` does not expose those).

4. [x] **Upgrade `ModuleStatus.Stats` shape.** Keep `Optional<PoolStats> pool()`. Drop the
   placeholder `Optional<List<String>> tables()`. Add
   `Optional<List<EntityStats>> entities()`. Hand-written builder.

5. [x] **Add `ConnectResponse.syncTopics: Map<String, String>`** in
   `app.l2nx.gs.adapter.api.rest`. Field is nullable on the wire (Gson default
   null). Update Gson deserialization tests.

6. [x] **Add `ConnectContext.syncTopics()` Phase-2 field** returning unmodifiable map
   view. Builder thread-through; immutable copy on construction.

7. [x] **Add typed `SyncEvent<T>`** in `app.l2nx.gs.adapter.api.kafka.sync.db`:
   `String entityName`, `long pk`, `String op` (`CREATED|UPDATED|DELETED`), `T payload`
   (nullable for tombstone), `Instant timestamp`. Hand-written builder.

8. [x] **Add `ClanDbDto`** in `app.l2nx.gs.adapter.api.kafka.sync.db`: `long clanId`,
   `String clanName`, `int clanLevel`, `Long leaderId`, `Long allyId`. Hand-written
   builder. (Co-located with `SyncEvent` in M7 — both DB-sync wire types live
   under the same sub-package.)

9. [x] **Define Tier-2 SPI interfaces** in `:nx-gs-adapter-api` package
   `app.l2nx.gs.adapter.api.spi`: `DbSchemaProvider` (`schemaName()`, `mappings()`),
   `EntityMapping<T>` (`entityName`, `tableName`, `pkColumn`, `hashedColumns`, `mapRow`,
   `dtoType`). No `tickInterval` / `strategy` / `windowCount` / `queryTimeout` fields
   — engine config is operator-owned per cdc-engine R15.

10. [x] **Bump `nx-gs-adapter-api` to 0.6.0** in build.gradle.kts. Tag deferred to M37.

#### Checkpoint — wire shape complete

`nx-gs-adapter-api-0.6.0` compiles with all new types; downstream consumers
(`:nx-gs-adapter-core`, `:nx-gs-db-sync-core`) target it via `composite include` for
the rest of the plan.

### Bootstrap plumbing — `:nx-gs-adapter-core`

11. [x] **Parse `ConnectResponse.syncTopics`** in adapter-core's `/connect` response
    handler. Treat null/absent as empty map (defensive). Existing
    `ConnectResponse` Gson type-adapter handles `Map<String, String>` natively; verify
    a unit test covers null + empty + populated cases.

12. [x] **Thread `syncTopics` through `ConnectContext`** built before module dispatch in
    `NxAdapter.start()`. Construct an `unmodifiableMap` copy at boundary; modules see
    the immutable view.

13. [x] **Wire smoke test in :nx-gs-adapter-core** — WireMock `/connect` returning a
    `syncTopics` map with one entry, assert downstream `ConnectContext.syncTopics()`
    reflects it identically.

### CDC engine — `:nx-gs-db-sync-core` package `app.l2nx.gs.db.sync.engine`

14. [x] **Add `fastutil-core` runtime dep** to `:nx-gs-db-sync-core` (`it.unimi.dsi:fastutil-core:8.5.x`,
    ~3 MB). Verify shadowJar still bundles only `:nx-gs-log` and does NOT relocate
    fastutil packages (consumers can use the same fastutil JAR if they want;
    classpath collision risk is low since fastutil-core is read-only data
    structures with no version-coupled API).

15. [x] **`EngineConfig`** value class + `EngineConfig.from(ConfigResolver)` static
    factory reading `l2nx.cdc-engine.tick-interval-seconds` (default 60),
    `rows-per-window` (default 500_000), `query-timeout-seconds` (default 10),
    `publish-flush-seconds` (default 5). One-shot read at engine start; cached for
    engine lifetime.

16. [x] **`SnapshotStore`** wrapping `Long2IntOpenHashMap` per entity, keyed by
    `entityName`. API: `getCrc(entity, pk)`, `putCrc(entity, pk, crc)`,
    `removeCrc(entity, pk)`, `keysInRange(entity, fromPk, toPk)` returning a `LongSet`,
    `clearEntity(entity)`. Wiped on `CdcEngine.stop()`.

17. [x] **`WindowPlanner`**. Method
    `plan(EntityMapping mapping, Connection conn, int rowsPerWindow)` runs `SELECT
    MIN(<pk>), MAX(<pk>) FROM <table>` (50ms target per cdc-engine SC1), returns
    `List<Window { fromPk, toPk }>`. Single-window degenerate case when
    `MAX - MIN + 1 <= rowsPerWindow`. Half-open intervals partitioning `[MIN, MAX]`.

18. [x] **`Phase1Hasher`**. Method `hash(Window window, EntityMapping mapping, Connection
    conn)` runs `SELECT <pk>, CRC32(CONCAT_WS(',', col1, col2, ...)) FROM <table>
    WHERE <pk> BETWEEN ? AND ?` inside `START TRANSACTION WITH CONSISTENT SNAPSHOT,
    READ ONLY`, returns `Long2IntMap` (current scan for window).
    `Statement.setQueryTimeout(engineConfig.queryTimeoutSeconds())`.
    Commit and close transaction immediately after result drained.

19. [x] **Diff (inline in `EntitySyncTask`).** Given `Long2IntMap currentScan` + `LongSet
    prevKeysInRange` + `SnapshotStore.getCrc` lookups, produce
    `ChangeSet { LongSet created, LongSet updated, LongSet deleted }`.

20. [x] **`Phase2Fetcher`**. Method `fetch(EntityMapping mapping, LongList pks,
    Connection conn)` chunks PK list at 1000 entries, runs `SELECT <fetchColumns>
    FROM <table> WHERE <pk> IN (?,?,...)` inside `START TRANSACTION WITH CONSISTENT
    SNAPSHOT, READ ONLY` per chunk, calls `mapping.mapRow(rs)` per row, returns
    `Long2ObjectMap<T>` (PK → DTO). Phase-2 missing rows: silent no-op (next cycle
    catches via Phase 1 diff per resolved decision).

21. [x] **`SyncEventPublisher`**. Method `publish(EntityMapping mapping, String op, long
    pk, T dto, String topic)` builds `SyncEvent<T>`, sends via
    `NxKafka.send(topic, key=longSerialize(pk), value=Gson(syncEvent))`. Tombstone for
    DELETED (`payload=null`). Returns `Future<RecordMetadata>` for end-of-cycle flush
    walk. Engine-side: idempotent producer enabled in nx-gs-kafka defaults.

22. [x] **`TopicResolver` SAM** + concrete impl bound to `ctx.syncTopics()`.
    `String resolveTopic(String entityName)` returns the platform-supplied topic name
    or null. Cached map snapshot at engine start; no re-resolution per cycle.

23. [x] **`EntitySyncTask`** orchestrating one cycle for a single entity. Sequence:
    - Borrow `Connection` from `JdbcConnectionSource`, `setReadOnly(true)`
    - `WindowPlanner.plan(mapping, conn, rowsPerWindow)` → list of windows
    - For each window:
        - `Phase1Hasher.hash(window, mapping, conn)` → currentScan
        - Diff against `SnapshotStore` for window's PK range → ChangeSet
        - `Phase2Fetcher.fetch(mapping, created∪updated, conn)` → PK→DTO map
        - For each PK in created/updated/deleted: `SyncEventPublisher.publish(...)`
          → record `Future<RecordMetadata>` keyed by PK in per-cycle in-flight map
    - End of cycle: walk in-flight futures with `publishFlushSeconds` budget. For
      each:
        - succeeded → advance `SnapshotStore` for that PK (created/updated → put new
          CRC; deleted → remove)
        - failed/timed-out → leave snapshot untouched; PK replayed on next cycle
    - Close connection (try-with-resources)

24. [x] **`EntityStatsTracker`** holding volatile `Map<String, EntityStats>`. Per cycle:
    `recordCycleResult(entityName, EntityStats {...})`. Reader path
    (`currentStatuses()`) returns immutable `List<EntityStats>` snapshot. `volatile`
    reference assignment on rebuild; reader sees fully-formed list.

25. [x] **`CdcEngine`** top-level. `start(provider, topicResolver, kafkaSender, jdbcSource,
    engineConfig, statsTracker)`:
    - Resolve topics for every mapping (warn + entity-DEGRADED for missing)
    - Spin up `ScheduledExecutorService` with one daemon thread per entity (thread
      name `nx-cdc-{schemaName}-{entityName}`, daemon=true)
    - First tick fires immediately (initial sync per cdc-engine R7); subsequent
      ticks at `tickIntervalSeconds`
    - Each tick wrapped in `SafeRunnable` (reuse from adapter-bootstrap)
    - `stop()` cancels schedulers, awaits brief in-flight wait, clears all snapshots

26. [x] **`ConfigResolutionLogger`** emitting cdc-engine R16 startup log block on
    `CdcEngine.start()` (after EngineConfig resolution + topic resolution):
    one engine-globals line listing `tickInterval`, `rowsPerWindow`,
    `queryTimeout`, `publishFlush` with `[operator-override | default]` source tags
    + one per-entity line listing `entityName → topic` (or `topic=<missing —
    entity DEGRADED>` if platform did not deliver).

27. [x] **DEGRADED triage** in `EntitySyncTask`:
    - Borrow failure / connection lost → entity DEGRADED, snapshot untouched
    - `SQLTimeoutException` mid-window → window skipped, transition entity
      DEGRADED for the cycle, continue to next window
    - Generic SQLException mid-window → entity DEGRADED, abort cycle, next tick
      retries from start
    - Missing topic for entity → entity DEGRADED every cycle, no Kafka publishes

28. [x] **Per-entity `consecutiveErrors`** counter. Increment on DEGRADED, reset to 0 on
    HEALTHY. Surfaces in `EntityStats`.

#### Checkpoint — engine compiles + diff/publish unit tests pass

Each engine class has a unit test exercising the happy path + one obvious failure
mode. End-to-end test deferred to M36.

### DbSyncModule wiring — `:nx-gs-db-sync-core`

29. [x] **`DbSyncModule.onConnect(ctx)`** body:
    - Read `ctx.syncTopics()`. If null/empty → log WARN ("no entity topics in
      ConnectResponse — db-sync has nothing to sync"), set internal state
      `DISABLED`, return. No engine, no scheduler, no SPI resolution attempted.
    - Otherwise: resolve `JdbcConnectionSource` via `ServiceLoader.load(...)` per
      jdbc-connection-source R2 (already-implemented Phase 1 path). Smoke borrow +
      `isValid(5)` + close.
    - Resolve `DbSchemaProvider` via `ServiceLoader.load(...)`: 0 → DISABLED +
      WARN; 1 → cache; >1 → FAILED + ERROR listing impl class names.
    - Cache `ConnectContext` ref for later `currentStatus()` calls (heartbeat
      thread).

30. [x] **`DbSyncModule.start()`** instantiates `CdcEngine` with cached
    `JdbcConnectionSource`, resolved provider's `mappings()`, `TopicResolver`
    bound to `ctx.syncTopics()`, `EngineConfig.from(configResolver)`,
    `EntityStatsTracker`, Kafka sender. `engine.start()`.

31. [x] **`DbSyncModule.currentStatus()`** override surfaces:
    - `pool()` from `JdbcConnectionSource.stats()` (Optional)
    - `entities()` from `EntityStatsTracker.currentStatuses()` (Optional)
    - `state` from internal module state (DISABLED / DEGRADED / FAILED / ACTIVE)

32. [x] **`DbSyncModule.stop()`**: `engine.stop()` (cancels schedulers, awaits brief
    in-flight wait). `onDisconnect()`: clear cached refs, snapshots already wiped
    by `engine.stop()`. Idempotent.

33. [x] **Module-level exception isolation** (db-sync R9 Phase 2). Verify every entry
    point (scheduler tick, Kafka producer callback, JdbcConnectionSource borrow
    failure) catches `Throwable`, logs via NxLog, transitions affected entity to
    DEGRADED — other entities continue. Module-level FAILED reserved for the >1
    DbSchemaProvider conflict + cdc-engine cannot construct.

### Bohpts impl — external repo `E:/bohpts/code/bohpts-core` (db-sync R10)

34. [x] **`BohptsDbSchemaProvider`** (operator-chosen package
    `l2e.gameserver.l2nx`): `schemaName() = "bohpts"`, `mappings()` returns
    `Collections.singletonList(new ClanMapping())`. Bohpts-core build only needs
    `app.l2nx:nx-gs-adapter-api:0.6.0` at compile-time — Tier-2 SPI lives in
    api. `nx-gs-db-sync-core` lands on the same host JVM classpath (alongside
    `nx-gs-adapter-core`) at runtime so its `ServiceLoader` can discover bohpts's
    provider, but bohpts-core's own source code does NOT import anything from
    `nx-gs-db-sync-core`.

35. [x] **`ClanMapping implements EntityMapping<ClanDbDto>`**:
    - `entityName() = "clan"`
    - `tableName() = "clan_data"`
    - `pkColumn() = "clan_id"`
    - `hashedColumns() = ["clan_name", "clan_level", "leader_id", "ally_id"]`
    - `dtoType() = ClanDbDto.class`
    - `mapRow(rs)`:
      ```
      ClanDbDto.builder()
          .clanId(rs.getLong("clan_id"))
          .clanName(rs.getString("clan_name"))
          .clanLevel(rs.getInt("clan_level"))
          .leaderId(nullIfZero(rs.getLong("leader_id")))
          .allyId(nullIfZero(rs.getLong("ally_id")))
          .build()
      ```
      Helper `Long nullIfZero(long v) { return v == 0L ? null : v; }`.

36. [x] **Service descriptor**
    `META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider` pointing to
    `l2e.gameserver.l2nx.BohptsDbSchemaProvider`.

### End-to-end smoke + publish

37. [x] **Integration test in `:nx-gs-db-sync-core`**: `CdcEngineE2ETest`.
    Testcontainers MySQL + `clan_data` fixture (3 rows), Testcontainers Kafka,
    WireMock platform delivering `syncTopics["clan"]="bohpts.gs.sync.clans"`. A
    test-scope `FakeBohptsDbSchemaProvider` registered via test
    `META-INF/services` resource. Assertions:
    - On engine first tick: 3 `SyncEvent { op: "CREATED" }` records on the topic,
      key = clan_id (long, 8 bytes), payload = ClanDbDto with all fields populated
    - After UPDATE one row's `clan_name` directly via JDBC: next cycle emits
      exactly 1 `op: "UPDATED"` with the new name in payload
    - After DELETE one row: next cycle emits exactly 1 `op: "DELETED",
      payload: null` (tombstone)
    - HeartbeatEvent.enabledModules contains
      `{name: "db-sync", state: "ACTIVE", stats: {pool: {...}, entities:
      [{name: "clan", state: "HEALTHY", rowCount: 3, ...}]}}`
    - `consecutiveErrors = 0` after a clean cycle

38. [pending — manual operator step, blocked on M39 publish + bohpts dev-branch
    `MailService` fix] **Manual smoke in bohpts-core dev environment.**
    - Drop `nx-gs-db-sync-core-0.1.0.jar` + `nx-gs-adapter-api-0.6.0.jar` into
      bohpts-core build (already pulls the SPI deps via Gradle).
    - Configure `l2nx.gs-key` + `l2nx.platform-url` pointing at staging.
    - Verify staging platform delivers
      `syncTopics["clan"]="bohpts.gs.sync.clans"` in `/connect` response (already
      configured per `nx-tenants` server-registration spec).
    - Boot bohpts-core; observe Kafka topic `bohpts.gs.sync.clans` receives N
      CREATED events (N = current `clan_data` row count) on first tick.
    - Modify a clan_data row in MySQL (e.g. `UPDATE clan_data SET clan_level =
      clan_level + 1 WHERE clan_id = 123`); next tick (~60s) should emit 1
      UPDATED event with the new level.
    - DELETE the row; next tick should emit 1 DELETED tombstone.
    - Tail bohpts-core logs for the cdc-engine R16 startup block + per-cycle
      heartbeats.

### Versions and publishing (db-sync R11; adapter-modules R8)

39. [pending — manual git-tag step] **Tag and publish.**
    - Tag `api/v0.6.0` → CI publishes `nx-gs-adapter-api-0.6.0` to Maven Central.
    - Tag `core/v0.3.2` → CI publishes `nx-gs-adapter-core-0.3.2` to Maven Central
      (carries the M11-M13 `syncTopics` plumbing through `ConnectResponse` /
      `ConnectContext`; pre-existing `core/v0.3.1` was tagged before that work
      and cannot be reused).
    - Tag `db-sync/v0.1.1` → CI publishes `nx-gs-db-sync-core-0.1.1` to Maven
      Central (carries the full M11-M37 CDC engine + e2e; pre-existing
      `db-sync/v0.1.0` was tagged at the M5-M10 boundary and cannot be reused).
    - Verify all three artifacts resolve from Maven Central before bohpts-core
      switches its `mavenLocal()` fallback off.
    - `:nx-gs-kafka` version stays at 0.2.0 (no wire change).

## Notes

- ...
