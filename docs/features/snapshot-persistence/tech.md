# Snapshot Persistence — tech

> Covers: [spec.md](./spec.md)

## Overview

Lives entirely in `:nx-gs-db-sync-core` under
`app.l2nx.gs.db.sync.engine.persist`. Boundary is the `SnapshotPersistence`
interface; default impl `FileSnapshotPersistence` writes per-entity binary
files `<persist-dir>/<schema>/<entityName>.snap` with tmp→rename atomicity,
a directory-level lock, and a trailing CRC32. `CdcEngine` calls
`persistence.load(snapshot)` once on start, `persistence.checkpoint(entity,
snapshot)` after every successful per-entity cycle (throttle is inside
the impl), and `persistence.flushAll(snapshot)` + `persistence.close()` on
stop before `SnapshotStore.clearAll()`. `DbSyncModule.buildPersistence`
constructs the impl from `EngineConfig`.

## Structure

- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/persist/`
    - `SnapshotPersistence.java` — boundary interface
      (`load` / `checkpoint` / `flushAll` / `close`)
    - `FileSnapshotPersistence.java` — disk impl
    - `NoopSnapshotPersistence.java` — test seam used by engine tests
      that don't need disk I/O
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/SnapshotStore.java`
  — extended with `entityNames()`, `forEachEntry(name, EntryConsumer)`,
  `newLoader(name, sizeHint)`, nested `SnapshotStore.EntryConsumer`
  primitive-typed SAM, nested `SnapshotStore.Loader` streaming bulk-load
  builder.
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/CdcEngine.java`
  — accepts `SnapshotPersistence` in ctor; lifecycle hooks at start /
  per-cycle / stop.
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/EngineConfig.java`
  — adds `KEY_PERSIST_DIR` + `KEY_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS`
  and accessors.
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/DbSyncModule.java`
  — `buildPersistence(config, schemaName)` factory; lock-acquisition
  failure → `STATE_FAILED`. `validateIdentifiers` extended to check
  `mapping.entityName()` + `provider.schemaName()` against `SqlIdent`
  regex.
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/engine/phase/ChangeSet.java`
  — `diff(...)` switched from `getCrc == MISSING_HASH` to
  `containsCrc(...)` to close the `0x80000000` CRC32 sentinel collision
  that this feature surfaces across restarts.

## Key components

- **`SnapshotPersistence`** — boundary interface. `load(SnapshotStore)`
  once on start; `checkpoint(entityName, SnapshotStore)` after every
  per-entity cycle (impl throttles); `flushAll(SnapshotStore)` and
  `close()` on shutdown. `checkpoint` for distinct entities may be called
  concurrently from different CDC pool workers — impls must be
  thread-safe on cross-entity bookkeeping.
- **`FileSnapshotPersistence`** (implements R1–R9) — disk impl. Per-entity
  binary file with magic `"NXSS"` + format version + entity name + entry
  count + dense `(long pk, int crc32)` pairs + trailing CRC32 of the
  body. Writes via `tmp → fsync → ATOMIC_MOVE` (R4 + R9). Per-entity
  throttle via `ConcurrentHashMap<String, Long>` of last-write `nanoTime`
  (R5). Directory lock via `FileChannel.tryLock` on `.lock`, fail-fast
  on contention (R6). Load skips bad files with WARN (R7); count sanity
  cap by file size before allocation prevents OOM on crafted headers.
- **`SnapshotStore.Loader`** — streaming bulk-load builder. `newLoader`
  pre-allocates the fastutil map once at `sizeHint`; caller `put(pk, crc)`
  per entry; `commit()` atomically publishes to the live store. Abandoned
  loaders (decode failure mid-stream) never reach the store. Eliminates
  the 78 MB transient `long[] + int[]` allocation a parallel-array load
  API would require.
- **`SnapshotStore.EntryConsumer`** — primitive-typed `(long, int)` SAM.
  `forEachEntry` iterates via fastutil's `fastIterator()` reusing one
  `Entry` view, so a 6.5M-entry dump allocates zero `Entry` instances.
- **`NoopSnapshotPersistence`** — test seam. Singleton `.INSTANCE` used by
  `CdcEngineTest` / `CdcEnginePoolTest` / `CdcEngineE2ETest` to avoid
  spinning up a temp directory per test.

## Data flows

**Engine start** (`CdcEngine.start`):

```
persistence.load(snapshot)   # restore per-entity maps from disk
ConfigResolutionLogger.log(...)
schedule per-entity ticks on shared pool
```

**Per-entity tick** (`CdcEngine.runGuardedCycle`):

```
task.runCycle()              # Phase 1 + diff + Phase 2 + publish + snapshot advance
statsTracker.recordCycleResult(entity, result)
persistence.checkpoint(entity, snapshot)   # throttled by R5; impl-internal
```

**Engine stop** (`CdcEngine.stop`):

```
cancel scheduled futures
pool.shutdownNow + awaitTermination
persistence.flushAll(snapshot)   # bypass throttle
persistence.close()              # release dir lock
snapshot.clearAll()              # wipe in-memory
```

**On cold restart** (orphan-detection scenario):

1. Adapter restarts; `persistence.load` populates `SnapshotStore` from
   prior `<entityName>.snap` files.
2. First tick runs Phase-1 against host DB. Rows deleted while offline
   are now absent from the live scan but present in `SnapshotStore`
   (loaded from disk).
3. `ChangeSet.diff` classifies them as DELETED via the
   `prevKeysInRange \ currentScan` set difference.
4. Phase-2 publishes a `SyncEvent { op: DELETED }` per orphaned PK.
5. `nx-gameservers` consumer removes the row from `l2nx` DB. Orphan
   closed.

## Data model

**Filesystem layout** (per host):

```
<persist-dir>/<schema>/
    .lock                                # FileChannel.tryLock target
    <entityName>.snap                    # committed snapshot
    <entityName>.snap.tmp                # transient — present only mid-write
```

`<persist-dir>` from `l2nx.cdc-engine.persist.dir` (default
`nx-cdc-snapshot`, relative to JVM cwd). `<schema>` from
`DbSchemaProvider.schemaName()` (validated against SQL-identifier regex
per R8). `<entityName>` likewise validated.

**Binary file format** (big-endian, per entity):

```
magic         : 4 bytes ASCII "NXSS"
version       : int16 = 1
entityNameLen : int16 (UTF-8 byte length, capped at MAX_ENTITY_NAME_BYTES=1024)
entityName    : entityNameLen bytes (UTF-8)
count         : int32 entry count
entries       : count × (int64 pk, int32 crc) — 12 bytes each
bodyCrc32     : int32 CRC32 of count + entries (raw bytes)
```

At 6.5M entries → ~78 MB on disk. Read/write throughput dominated by
fsync, not encode/decode.

## Decisions

- **Always on, no enable flag.** The orphan-on-restart bug is a
  correctness issue, not a perf knob. An `enabled=false` switch invites
  operators to ship the bug. Two operational knobs (`dir`,
  `checkpoint-min-interval-seconds`) cover deployment differences.

- **Per-entity file, full overwrite per checkpoint, no WAL.** Considered
  append-only WAL + periodic compaction. Rejected — at 78 MB / 5 min /
  entity, a single-write-per-checkpoint is simpler, atomic via tmp→rename,
  no replay code path needed. Trade-off: per-flush I/O proportional to
  entity size, not delta size.

- **`fsync` on CDC pool thread.** Considered a dedicated
  `nx-cdc-persist-<schema>` daemon to decouple fsync latency from the
  next CDC tick on the same pool worker. Rejected for now — see R10 in
  spec. Decoupling requires snapshot copy-on-handoff to preserve
  `SnapshotStore`'s single-writer-per-entity contract, which reintroduces
  the 78 MB transient allocation the streaming dump was designed to avoid.
  Will revisit if real-world latency-tail measurements show pool
  utilization above ~30% in production.

- **Directory lock via `FileChannel.tryLock`, fail-fast → STATE_FAILED.**
  Silent corruption of two adapters racing on the same dir is far worse
  than a loud refuse-to-start. Operators see actionable error message in
  the failure log.

- **`ChangeSet.diff` uses `containsCrc` instead of sentinel comparison.**
  Pre-fix, a real CRC32 value `0x80000000` (Integer.MIN_VALUE) collided
  with `Phase1Hasher.MISSING_HASH`, making the row perpetually
  re-CREATED on every cycle. The persisted value round-trips correctly
  through `Loader` → `Long2IntOpenHashMap`, but the downstream consumer
  predicate was buggy; persistence made the latent bug observable across
  restarts. Fix is in the diff predicate (one place, one line), not the
  storage layer.

- **Binary JDK-only format, no SQLite / RocksDB / MapDB.** CLAUDE.md
  "minimum dependencies" rule. `DataInputStream`/`DataOutputStream` +
  `ByteBuffer` covers the format with zero new deps. Trade-off: not
  human-inspectable; mitigated by the unit tests' `RandomAccessFile`-
  based corruption fixtures that document the layout.

- **Throttle measured "since write completed", not "since write began".**
  On a slow fsync this stretches the effective window past the configured
  value. Intended semantics — "don't double-fsync within N seconds of
  completion" — keeps adapter from disk-thrashing when one write is
  already in-flight.

- **`flushAll` bypasses throttle on shutdown.** Freshest state always
  survives. Worst case: N entity writes back-to-back on engine stop, all
  atomic — adapter shutdown takes seconds, not minutes.

- **Sanity cap on `count` during load.** Crafted header
  (`count=Integer.MAX_VALUE`) would trigger OOM in
  `Long2IntOpenHashMap(count)` allocation BEFORE the checksum check could
  reject the body. Cap `count` by `(fileSize - 4) / ENTRY_BYTES`.

## Integration points

- **`CdcEngine`** — receives `SnapshotPersistence` via constructor;
  drives the lifecycle (load on start, checkpoint per cycle, flushAll +
  close on stop). All persistence errors are caught and logged WARN; a
  failed load just leaves the snapshot empty (legacy MVP behavior), a
  failed checkpoint is a no-op, a failed `flushAll` doesn't block
  shutdown.

- **`DbSyncModule.buildPersistence`** — constructs
  `FileSnapshotPersistence(Paths.get(config.persistDir()).resolve(schemaName),
  config.persistCheckpointMinIntervalSeconds())`. A `RuntimeException` from
  the constructor (e.g. directory unwritable, lock held by another JVM)
  transitions the module to `STATE_FAILED` before the engine starts.

- **`EngineConfig`** — two new keys under the existing `l2nx.cdc-engine.*`
  namespace:
    - `l2nx.cdc-engine.persist.dir` (string, default `nx-cdc-snapshot`)
    - `l2nx.cdc-engine.persist.checkpoint-min-interval-seconds` (int,
      default `300`)
      Reading uses the same file-first / sysprop-fallback chain as the rest
      of the engine config.

- **`SqlIdent`** — reused for `entityName` + `schemaName` path-safety
  validation in `DbSyncModule`. Same regex
  `^[A-Za-z_][A-Za-z0-9_]{0,63}$` that protects SQL identifier
  interpolation now also protects filesystem path interpolation.

- **`ChangeSet.diff`** — the only consumer of the
  `getCrc(...) == MISSING_HASH` predicate; switched to
  `SnapshotStore.containsCrc(...)`.

## Extension points

- **Alternative storage backend** — implement `SnapshotPersistence`, wire
  via `DbSyncModule.buildPersistence`. The interface contract (load /
  checkpoint / flushAll / close) is small and storage-agnostic; an
  in-process embedded KV (LevelDB, MapDB) or a remote object store
  (S3, MinIO) would fit. Throttle semantics + lifecycle ordering would
  carry over.

- **Per-entity tuning** — the throttle is currently engine-global. Per-
  entity overrides (e.g. `l2nx.cdc-engine.persist.checkpoint-min-interval-
  seconds.<entityName>`) would slot into `EngineConfig.from` with the
  same file-first chain as today's engine knobs.
