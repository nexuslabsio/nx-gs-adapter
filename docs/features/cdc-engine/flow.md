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
│                                       ││   │  one daemon thread       │  │  │
│                                       ││   │  per entity:             │  │  │
│                                       ││   │  nx-cdc-{schema}-{name}  │  │  │
│                                       ││   └──┬───────────────────────┘  │  │
│                                       ││      │ schedules tick           │  │
│                                       ││      ▼                          │  │
│                                       ││   ┌──────────────────────────┐  │  │
│                                       ││   │     EntitySyncTask       │  │  │
│                                       ││   │  one per EntityMapping   │  │  │
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
│  │ plan(...)       │  → List<Window>  (chunks of rowsPerWindow)                 │
│  └────────┬────────┘                                                            │
│           │                                                                     │
│           ▼   ┌─────────────────────── for each Window ────────────────────┐    │
│           ╰─▶ │                                                            │    │
│               │  ┌──────────────────┐                                      │    │
│               │  │ Phase1Hasher     │  START TX WITH CONSISTENT SNAPSHOT,  │    │
│               │  │ hash(window)     │       READ ONLY                      │    │
│               │  │                  │  SELECT pk, CRC32(CONCAT_WS(...))    │    │
│               │  │                  │  WHERE pk BETWEEN ? AND ?            │    │
│               │  └────────┬─────────┘  → Long2IntMap currentScan           │    │
│               │           │                                                │    │
│               │           ▼                                                │    │
│               │  ┌──────────────────┐  ┌──────────────────┐                │    │
│               │  │ ChangeSet.diff   │◀─│ SnapshotStore    │ prev CRCs      │    │
│               │  │                  │  │ keysInRange(...) │ in window      │    │
│               │  │ → created   set  │  │ (AVL subMap)     │                │    │
│               │  │ → updated   set  │  └──────────────────┘                │    │
│               │  │ → deleted   set  │                                      │    │
│               │  └────────┬─────────┘                                      │    │
│               │           │                                                │    │
│               │           ▼  (created ∪ updated)                           │    │
│               │  ┌──────────────────┐                                      │    │
│               │  │ Phase2Fetcher    │  SELECT * WHERE pk IN (?,...,?)      │    │
│               │  │ fetch(...)       │  chunks of 1000, last padded         │    │
│               │  │                  │  → Long2ObjectMap<pk, DTO>           │    │
│               │  └────────┬─────────┘                                      │    │
│               │           │                                                │    │
│               │           ▼                                                │    │
│               │  ┌──────────────────┐                                      │    │
│               │  │ SyncEventPublisher                                      │    │
│               │  │ per-PK publish:  │                                      │    │
│               │  │  • CREATED → SyncEvent(payload=DTO)                     │    │
│               │  │  • UPDATED → SyncEvent(payload=DTO)                     │    │
│               │  │  • DELETED → null (tombstone)                           │    │
│               │  │ key = 8-byte big-endian pk                              │    │
│               │  │ returns Future<RecordMetadata>                          │    │
│               │  └────────┬─────────┘                                      │    │
│               │           │                                                │    │
│               │           ▼                                                │    │
│               │   inFlight: Long2ObjectMap<pk, Future>                     │    │
│               │   pendingCreates / pendingDeletes / pendingCrcAdvance      │    │
│               │                                                            │    │
│               └──────────────── next window ───────────────────────────────┘    │
│                                                                                 │
│           ▼  windows done                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                walkInFlightAndAdvance — budget = publishFlushSeconds    │   │
│  │                                                                         │   │
│  │  for each (pk, future) in inFlight:                                     │   │
│  │    ┌──────────────┐    ok      ┌─────────────────────────────────┐      │   │
│  │    │ isDone()?    │ ─────────▶ │ SnapshotStore advance:          │      │   │
│  │    │              │            │   create → putCrc + count++     │      │   │
│  │    │              │            │   update → putCrc + count++     │      │   │
│  │    │              │            │   delete → removeCrc + count++  │      │   │
│  │    └──────┬───────┘            └─────────────────────────────────┘      │   │
│  │           │ pending                                                     │   │
│  │           ▼                                                             │   │
│  │    ┌──────────────┐  budget left  ┌─────────────┐                       │   │
│  │    │ future.get   │ ────────────▶ │ wait         │ ─▶ same advance      │   │
│  │    │ (remaining)  │               │ remainingNs │                       │   │
│  │    └──────┬───────┘  exhausted    └─────────────┘                       │   │
│  │           │                                                             │   │
│  │           ▼                                                             │   │
│  │    ┌──────────────────────────────────────────────────────────────┐     │   │
│  │    │ leave snapshot untouched → diff re-detects next cycle,       │     │   │
│  │    │ publish replays. one summary log per cycle, not per PK.      │     │   │
│  │    └──────────────────────────────────────────────────────────────┘     │   │
│  └────────────────────────────────────┬────────────────────────────────────┘   │
│                                       │                                         │
│                                       ▼                                         │
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
