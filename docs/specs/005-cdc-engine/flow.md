# CDC Engine — Block Diagrams

Plain block diagrams. See [`spec.md`](./spec.md) for requirements,
[`tech.md`](./tech.md) for class-level details.

## Component layout

```
┌─────────────────────────────── Host JVM ───────────────────────────────────┐
│                                                                             │
│   ┌─── nx-gs-adapter-core ───┐         ┌─── nx-gs-db-sync-core ──────────┐  │
│   │                          │         │                                 │  │
│   │   NxAdapter              │         │   DbSyncModule                  │  │
│   │     │                    │         │     │                           │  │
│   │     ├─ ConnectFlow ──────┼─HTTP───▶│     ├─ onConnect(ctx)           │  │
│   │     │                    │         │     │    resolves SPIs          │  │
│   │     ├─ HeartbeatService ◀┼────────┐│     │                           │  │
│   │     │                    │        ││     ├─ start                    │  │
│   │     └─ ModuleRegistry ───┼─SL────▶││     │    builds CdcEngine       │  │
│   │                          │        ││     │                           │  │
│   └──────────────────────────┘        ││     └─ stop / onDisconnect      │  │
│                                       ││           │                     │  │
│                                       ││           ▼                     │  │
│                                       ││   ┌──────────────────────────┐  │  │
│                                       ││   │       CdcEngine          │  │  │
│                                       ││   │  shared daemon pool:     │  │  │
│                                       ││   │  nx-cdc-pool-<schema>-N  │  │  │
│                                       ││   │  size = workers config   │  │  │
│                                       ││   └──┬───────────────────────┘  │  │
│                                       ││      │ scheduleWithFixedDelay   │  │
│                                       ││      ▼                          │  │
│                                       ││   ┌──────────────────────────┐  │  │
│                                       ││   │     EntitySyncTask       │  │  │
│                                       ││   │  one per EntityMapping   │  │  │
│                                       ││   │  AtomicBoolean ticking   │  │  │
│                                       ││   └─┬───────┬───────┬────────┘  │  │
│                                       ││     │       │       │           │  │
│                                       ││     ▼       ▼       ▼           │  │
│                                       ││  Snapshot  Stats   KafkaSender  │  │
│                                       ││   Store    Tracker  (byte[]-key)│  │
│                                       │└──────────────│──────────│───────┘  │
│                                       │               │          │          │
│                                       └───────────────┘          │          │
│                                                                  │          │
│   ┌── Tier-3 SPI ──┐    ┌── Tier-2 SPI ──┐                       │          │
│   │ JdbcConnection │    │ DbSchema       │                       │          │
│   │ Source         │    │ Provider       │                       │          │
│   │  (bohpts impl) │    │  (bohpts impl) │                       │          │
│   └────────┬───────┘    └────────────────┘                       │          │
│            │                                                     │          │
└────────────┼─────────────────────────────────────────────────────┼──────────┘
             │                                                     │
             ▼                                                     ▼
        ┌─────────┐                                          ┌──────────┐
        │  MySQL  │                                          │  Kafka   │
        │ (game)  │                                          │ (per-    │
        └─────────┘                                          │  tenant) │
                                                             └──────────┘
```

`SL` = `ServiceLoader`. ModuleRegistry discovers `DbSyncModule`; `DbSyncModule`
discovers `JdbcConnectionSource` + `DbSchemaProvider` independently.

## Per-cycle algorithm (one entity, one tick)

```
┌─────────────────────────── EntitySyncTask.runCycle() ──────────────────────────┐
│                                                                                 │
│  ┌─────────────────┐  already        ┌────────────────┐                         │
│  │ ticking.CAS     │ ───────────────▶│  WARN + skip   │    overlap guard:       │
│  │ false → true    │                 │  (no record)   │    previous tick busy   │
│  └────────┬────────┘                 └────────────────┘                         │
│           │ acquired                                                            │
│           ▼                                                                     │
│  ┌─────────────────┐  no topic       ┌────────────────┐                         │
│  │ TopicResolver   │ ───────────────▶│  DEGRADED      │ ─▶ recordCycleResult    │
│  │ resolveTopic    │                 │  no publish    │    (skip rest)          │
│  └────────┬────────┘                 └────────────────┘                         │
│           │ topic ok                                                            │
│           ▼                                                                     │
│  ┌─────────────────┐  borrow fail    ┌────────────────┐                         │
│  │ JdbcSource      │ ───────────────▶│  DEGRADED      │ ─▶ recordCycleResult    │
│  │ getConnection   │                 │  snapshot ✕    │                         │
│  └────────┬────────┘                 └────────────────┘                         │
│           │ Connection                                                          │
│           ▼                                                                     │
│  ┌─────────────────┐                                                            │
│  │ WindowPlanner   │  SELECT MIN(pk), MAX(pk) FROM <table>                      │
│  │ plan(...)       │  + snapshot.minPk/maxPk (O(1), incrementally tracked)      │
│  │                 │  → List<Window>  (chunks of rowsPerWindow)                 │
│  └────────┬────────┘                                                            │
│           │                                                                     │
│           ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐        │
│  │ snapshot.bucketByWindows(plan)                                      │        │
│  │ one pass over snapshot + binary search on plan boundaries           │        │
│  │ → per-window cache for O(1) keysInRange lookups inside the loop     │        │
│  └─────────────────────────────────────────────────────────────────────┘        │
│           │                                                                     │
│           ▼   ┌─────────────────────── for each Window ────────────────────┐    │
│           ╰─▶ │                                                            │    │
│               │  ┌──── Phase 1: ONE consistent-snapshot txn ───────────┐   │    │
│               │  │ ConsistentSnapshotTxn.runReadOnly(conn, () -> {     │   │    │
│               │  │   START TX WITH CONSISTENT SNAPSHOT, READ ONLY      │   │    │
│               │  │                                                     │   │    │
│               │  │   Phase1Hasher.hashPrimary(w, ...)                  │   │    │
│               │  │     SELECT pk, CRC32(...) BETWEEN ? AND ?           │   │    │
│               │  │     setFetchSize(cfg.fetchSize)  ← cursor mode      │   │    │
│               │  │     registered in StatementRegistry                 │   │    │
│               │  │                                                     │   │    │
│               │  │   for each ChildSource:                             │   │    │
│               │  │     Phase1Hasher.hashChild(w, ...)                  │   │    │
│               │  │       SELECT fk, BIT_XOR(CRC32(...)) GROUP BY fk    │   │    │
│               │  │     XOR-fold into currentScan keyed by primary PK   │   │    │
│               │  │                                                     │   │    │
│               │  │   ChangeSet.diff(currentScan,                       │   │    │
│               │  │                  snapshot.keysInRange(w))           │   │    │
│               │  │ })  COMMIT (or rollback on Throwable)               │   │    │
│               │  └─────────────────────────────────────────────────────┘   │    │
│               │           │                                                │    │
│               │           ▼  (created ∪ updated)                           │    │
│               │  ┌──── Phase 2: SEPARATE consistent-snapshot txn ────┐     │    │
│               │  │  ConsistentSnapshotTxn.runReadOnly(conn, () -> {  │     │    │
│               │  │    START TX WITH CONSISTENT SNAPSHOT, READ ONLY   │     │    │
│               │  │    (fresh post-Phase-1 view — intentional;        │     │    │
│               │  │     row deleted between phases = silent no-op,    │     │    │
│               │  │     detected next cycle)                          │     │    │
│               │  │                                                   │     │    │
│               │  │    Phase2Fetcher.fetchPrimary / fetchChild        │     │    │
│               │  │      SELECT * WHERE pk IN (?,...,?) chunks=1000   │     │    │
│               │  │      setFetchSize + StatementRegistry             │     │    │
│               │  │                                                   │     │    │
│               │  │    SyncEventPublisher per pk:                     │     │    │
│               │  │      CREATED / UPDATED → SyncEvent(payload=DTO)   │     │    │
│               │  │      DELETED           → SyncEvent(payload=null)  │     │    │
│               │  │                          (NOT a Kafka tombstone — │     │    │
│               │  │                          bounded retention topic) │     │    │
│               │  │      key = 8-byte big-endian pk                   │     │    │
│               │  │  })  COMMIT                                       │     │    │
│               │  └───────────────────────────────────────────────────┘     │    │
│               │           │                                                │    │
│               │           ▼                                                │    │
│               │   inFlight: Long2ObjectMap<pk, Future>  (window-scoped)    │    │
│               │   pendingCreates / pendingDeletes / pendingCrcAdvance      │    │
│               │                                                            │    │
│               │           ▼                                                │    │
│               │  ┌──────────────────────────────────────────────────────┐  │    │
│               │  │ walkInFlightAndAdvance — TWO PASSES                   │  │    │
│               │  │  budget = publishFlushSeconds                        │  │    │
│               │  │                                                      │  │    │
│               │  │  pass 1 — drain isDone() (cheap, non-blocking):      │  │    │
│               │  │    ok       → SnapshotStore advance                  │  │    │
│               │  │    failed   → leave snapshot untouched → replay      │  │    │
│               │  │                                                      │  │    │
│               │  │  pass 2 — for still-pending: f.get(remainingNs, NS)  │  │    │
│               │  │    same per-PK outcomes; deadline shared across all  │  │    │
│               │  │                                                      │  │    │
│               │  │  done-first ordering avoids HoL blocking on a slow   │  │    │
│               │  │  ack starving later already-acked publishes          │  │    │
│               │  └──────────────────────────────────────────────────────┘  │    │
│               │  inFlight + pending* now GC-eligible — capping cycle-     │    │
│               │  resident heap at one window's worth, not the whole       │    │
│               │  cycle's. (per-window flush.)                              │    │
│               │                                                            │    │
│               └──────────────── next window ───────────────────────────────┘    │
│                                                                                 │
│           ▼  windows done                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ EntityStatsTracker.recordCycleResult(entityName, CycleResult)           │   │
│  │   state ∈ {HEALTHY, DEGRADED}                                           │   │
│  │   durationMs, created/updated/deleted, rowCount, consecutiveErrors      │   │
│  │   ↑ HeartbeatService reads via currentStatuses() each heartbeat tick    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Snapshot evolution (worked example, 4 cycles)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Cycle 1 — initial sync (snapshot empty)                                    │
│  ──────────────────────────────────────────────────────                     │
│  DB rows:    {1:100, 2:200, 3:300}                                          │
│  prev:       ∅                                                              │
│  diff:       created={1,2,3}                                                │
│  Kafka:      CREATED pk=1, pk=2, pk=3   → all ack'd                         │
│  snapshot:   {1:100, 2:200, 3:300}      ← advanced                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Cycle 2 — pk=2 updated, pk=3 deleted, pk=4 inserted                        │
│  ──────────────────────────────────────────────────────                     │
│  DB rows:    {1:100, 2:250, 4:400}                                          │
│  prev:       {1,2,3}                                                        │
│  diff:       updated={2}  created={4}  deleted={3}                          │
│  Kafka:      UPDATED pk=2 (DTO) | CREATED pk=4 (DTO) | DELETED pk=3 (null)  │
│              all ack'd                                                      │
│  snapshot:   {1:100, 2:250, 4:400}      ← advanced                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Cycle 3 — pk=2 updated again, but Kafka stalls past flush deadline         │
│  ──────────────────────────────────────────────────────                     │
│  DB rows:    {1:100, 2:260, 4:400}                                          │
│  prev:       {1,2,4}                                                        │
│  diff:       updated={2}                                                    │
│  Kafka:      UPDATED pk=2  → ack delayed > publishFlushSeconds              │
│  snapshot:   {1:100, 2:250, 4:400}      ← pk=2 NOT advanced (still 250)     │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Cycle 4 — replay pk=2 (snapshot still 250, DB still 260)                   │
│  ──────────────────────────────────────────────────────                     │
│  DB rows:    {1:100, 2:260, 4:400}                                          │
│  prev:       {1,2,4}                                                        │
│  diff:       updated={2}                ← same row re-detected              │
│  Kafka:      UPDATED pk=2  → ack ok now                                     │
│  snapshot:   {1:100, 2:260, 4:400}      ← advanced                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

The double-publish of `pk=2` (cycle 3 + cycle 4) is benign — platform-side
consumer is idempotent (log compaction de-dupes by key, application logic
de-dupes by `(entityName, pk)`).

## Module + entity state

```
DbSyncModule (heartbeat-visible state)

         ┌──────────────┐
         │     INIT     │
         └──────┬───────┘
                │ onConnect(ctx)
                ▼
   ┌──────────────────────────────┐
   │  decision tree:              │
   │  • syncTopics empty?         │ → DISABLED
   │  • 0 / >1 JdbcSource?        │ → FAILED
   │  • 0 / >1 SchemaProvider?    │ → DISABLED / FAILED
   │  • smoke check passed?       │ → ACTIVE
   │  • smoke check failed?       │ → DEGRADED  (engine still runs)
   └──────────────────────────────┘
                │
                │ start()
                ▼  (only if ACTIVE / DEGRADED)
        ┌──────────────┐
        │ engine.start │ → engine.start throws → FAILED
        └──────────────┘

EntityState (per-entity, surfaced in Stats.entities[])

   HEALTHY ◀──── clean cycle ────╮
      │                           │
      │ borrow fail / topic       │
      │ missing / SQL error       │
      ▼                           │
   DEGRADED ──── any clean cycle ─╯
      │
      │ consecutiveErrors keeps climbing if errors persist
```

## Shutdown / cancellation path

```
CdcEngine.stop()
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  for each EntitySyncTask:                                                   │
│    task.statementRegistry().cancelAll()                                     │
│       └─▶ for each tracked Statement: Statement.cancel()                    │
│                                                                             │
│  Thread.interrupt() alone is ignored by most JDBC drivers — a 12M-row       │
│  Phase 1 scan would otherwise keep running until natural completion.        │
│  Statement.cancel() is the portable way to actually abort an in-flight      │
│  query.                                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  pool.shutdown()                                                            │
│  pool.awaitTermination(drainBudget, SECONDS)                                │
│    ├─ success → done                                                        │
│    └─ timeout → WARN; pool.shutdownNow()                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Window planning at scale (bohpts x20 reference)

```
clan_data        characters       items
1k rows          152k rows        12.2M rows
─────────        ─────────        ─────────
[1, 1069]        [1, 152088]      [1, 12_200_000]

with rowsPerWindow = 500_000 (default):

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐
│  1 window        │  │  1 window        │  │  25 windows                  │
│  [1, 1069]       │  │  [1, 152088]     │  │  [1, 500000]                 │
│                  │  │                  │  │  [500001, 1000000]           │
│                  │  │                  │  │  ...                         │
│                  │  │                  │  │  [12000001, 12200000]        │
└──────────────────┘  └──────────────────┘  └──────────────────────────────┘
   sub-second           1-2 sec              all 25 walked back-to-back
                                             in one cycle
```

Cap: `MAX_WINDOWS_PER_PLAN = 1_000_000` — protects host JVM against
pathological PK ranges (BIGINT overflow + tiny `rowsPerWindow`).
