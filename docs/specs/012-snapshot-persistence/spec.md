# Snapshot Persistence

## Problem

The CDC engine's [`SnapshotStore`](../005-cdc-engine/spec.md) (PK → CRC32 per
entity) was heap-only — every adapter restart cleared it. On the next cold
start the engine fell back to initial-sync (cdc-engine R7), but more
critically: a row deleted from the host DB **while the adapter was offline**
was never observable. The next cycle's `ChangeSet.diff` compared the live
scan against an empty snapshot, classified everything as CREATE, and never
emitted the DELETE — `nx-gameservers` kept the orphan row in `l2nx` DB
forever.

This slice implements [cdc-engine R18](../005-cdc-engine/spec.md) (post-MVP
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
    + periodic compaction; rejected — at 78 MB per 5 min per entity, single-
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
  [`docs/specs/005-cdc-engine/spec.md`](../005-cdc-engine/spec.md) — R18
  promoted from `[todo]` post-MVP slot to `[done]` by this feature.
- Sibling feature (wires `FileSnapshotPersistence` into `DbSyncModule`):
  [`docs/specs/003-db-sync/spec.md`](../003-db-sync/spec.md).
