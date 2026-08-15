# Snapshot Persistence

## Problem

The CDC engine's [`SnapshotStore`](005-cdc-engine/spec.md) (PK → CRC32 per
entity) was heap-only — every adapter restart cleared it. On the next cold
start the engine fell back to initial-sync (cdc-engine R7), but more
critically: a row deleted from the host DB **while the adapter was offline**
was never observable. The next cycle's `ChangeSet.diff` compared the live
scan against an empty snapshot, classified everything as CREATE, and never
emitted the DELETE — `nx-gameservers` kept the orphan row in `l2nx` DB
forever.

This slice implements [cdc-engine R18](005-cdc-engine/spec.md) (post-MVP
persisted snapshot cache) so that the previous PK set survives JVM restart
and the next cycle's diff produces correct DELETE events for everything
removed during downtime.

Audience: db-sync operators (the persist directory is now part of the
deployment surface), engine internals owners (lifecycle changes on
`CdcEngine.start` / `stop`).

## Requirements

**Must:**

- [done] R1. The store MUST flush each entity's PK → CRC32 snapshot to disk
  after every successful CDC cycle (subject to per-entity throttle from R5).

- [done] R2. The store MUST load any existing snapshot files on engine start,
  BEFORE the first tick is scheduled, so the next cycle's diff against the
  source DB produces correct DELETE events for rows removed while the
  adapter was offline.

- [done] R3. The store MUST force-flush every tracked entity on
  `CdcEngine.stop()`, bypassing throttle, BEFORE `SnapshotStore.clearAll()`
  wipes the in-memory state.

- [done] R4. A snapshot write MUST survive `kill -9` / power loss mid-write:
  a crash between the start of a write and the publication MUST leave the
  previous committed snapshot intact. Mechanism: write to
  `<entityName>.snap.tmp`, `fsync`, then `Files.move(ATOMIC_MOVE,
REPLACE_EXISTING)`.

- [done] R5. Per-entity checkpoint rate MUST be throttled by
  `l2nx.cdc-engine.persist.checkpoint-min-interval-seconds` (default 300s) —
  a second checkpoint within that window after the previous write completed
  is a no-op. `flushAll` on shutdown bypasses the throttle.

- [done] R6. The store MUST refuse a second writer on the same persist
  directory: a directory-level file lock (`FileChannel.tryLock` on
  `<persist-dir>/<schema>/.lock`) fails fast on contention, transitioning
  `db-sync` to `STATE_FAILED` with an actionable error message.

- [done] R7. Corrupted / truncated / bad-magic / unsupported-version / bad-
  checksum / impossibly-large-count snapshot files MUST be logged WARN and
  skipped on load. The affected entity starts with an empty snapshot
  (graceful degradation back to legacy MVP cold-start behavior); other
  entities are unaffected.

- [done] R8. Provider-supplied `entityName` and `schemaName` MUST pass the
  SQL-identifier regex `^[A-Za-z_][A-Za-z0-9_]{0,63}$` before being
  interpolated into the file path. A Tier-2 SPI returning
  `entityName() = "../boom"` MUST fail the module at start, not silently
  traverse outside the persist directory.

**Should:**

- [done] R9. `flushAll` and `checkpoint` SHOULD `fsync` (via
  `FileDescriptor.sync()`) before the atomic rename so a kernel-side
  buffer-flush stall between writes and rename does not leave torn data
  visible after the rename.

**Could:**

- [todo] R10. Snapshot writes COULD move off the CDC pool thread onto a
  dedicated `nx-cdc-persist-<schema>` daemon to decouple fsync latency from
  the next CDC tick. Deferred — measured worst-case pool utilization is
  <3.5% even on pathologically slow disks (network mount + 4 entities ×
  78 MB × 12 writes/h), and the decoupling requires snapshot copy-on-handoff
  to preserve `SnapshotStore`'s single-writer-per-entity invariant, which
  reintroduces the 78 MB transient allocation that the streaming dump
  (R1 implementation) was designed to avoid.

**Non-goals:**

- **Freshest-mutation durability.** The throttle window (default 5 min)
  bounds loss. A crash between the last successful flush and the crash
  itself replays diffs since the last flushed snapshot via the next cycle's
  Phase-1 scan, NOT via a WAL replay. Persistence is a starvation cure for
  full-replay on every restart, not a real-time durability ledger.

- **Multi-node coordination.** The directory lock is host-scoped (one OS
  inode). Two adapter JVMs on DIFFERENT hosts pointed at a shared
  filesystem (NFS / SMB) are out of scope — operator deploys one adapter
  per host.

- **Schema-version invalidation.** The file format carries a single
  `FORMAT_VERSION` int; bumping it drops every existing snapshot. Column-
  set / DTO-shape changes that don't bump `FORMAT_VERSION` are caught by
  the natural re-CRC of the next cycle (rows with changed hash → UPDATE
  events), not by a per-entity fingerprint stored alongside the snapshot.

- **Persistence toggle.** Feature is always on; no `enabled` knob. Making
  it optional invites operators to ship the orphan-detection bug.

- **Compaction / WAL / per-row delta writes.** One file per entity, full
  overwrite per checkpoint, atomic publication. Considered append-only WAL
  - periodic compaction; rejected — at 78 MB per 5 min per entity, single-
    write-per-checkpoint is simpler with zero replay code path.

### Edge cases

- **Persist dir does not exist** → created via `Files.createDirectories` at
  `FileSnapshotPersistence` construction.
- **Disk full mid-write** → write fails, tmp file deleted, prior committed
  snapshot untouched, WARN logged, next throttle window retries.
- **Checksum mismatch on load** → file skipped, entity starts empty
  (legacy cold-start path).
- **Truncated file** → `EOFException` caught, file skipped.
- **`entityName` contains `..` / `/`** → rejected at module start via
  `SqlIdent.validate`, module → `STATE_FAILED` before any disk I/O.
- **No snapshot files present** (first start) → load is no-op, engine
  falls back to MVP cold-start initial-sync.
- **`FORMAT_VERSION` on disk doesn't match runtime** → skipped with WARN,
  full resync on first cycle.
- **Second adapter JVM on same dir** → second instance's
  `FileChannel.tryLock` returns `null` → `IllegalStateException` →
  `db-sync` state = `FAILED`.
- **CRC32 value of a real row equals `Phase1Hasher.MISSING_HASH`
  (`Integer.MIN_VALUE`)** → fixed in `ChangeSet.diff` to use
  `SnapshotStore.containsCrc` instead of sentinel comparison; the persisted
  value round-trips correctly.

## Open questions

- [assumed: Default throttle `300s` balances per-cycle I/O cost
  (~78 MB / 5 min / entity on a 6.5M-row table) against orphan-detection
  latency. Operators with rare restarts and cheap SSD can lower to 60s;
  with slow networked storage raise to 900s.]
- [assumed: Default persist dir `nx-cdc-snapshot` is relative to the JVM
  cwd — same convention as `l2nx.properties` discovery. Operators wanting
  persistence across container redeploys mount an explicit absolute path.]

## Links

- Parent feature (consumes `SnapshotPersistence` via `CdcEngine` ctor):
  [`docs/specs/005-cdc-engine/spec.md`](005-cdc-engine/spec.md) — R18
  promoted from `[todo]` post-MVP slot to `[done]` by this feature.
- Sibling feature (wires `FileSnapshotPersistence` into `DbSyncModule`):
  [`docs/specs/003-db-sync/spec.md`](003-db-sync/spec.md).

---

## Technical design

### Overview

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

### Structure

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

### Key components

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

### Data flows

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

### Data model

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

### Decisions

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

### Integration points

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

### Extension points

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
