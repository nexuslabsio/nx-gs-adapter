package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import it.unimi.dsi.fastutil.longs.*;

/**
 * Diff result of a single window's Phase-1 scan: PKs partitioned into
 * created (new in this scan), updated (CRC32 differs from snapshot), and
 * deleted (in snapshot for this window's PK range, missing in current scan).
 *
 * <p>The {@code currentScan} field carries the just-read CRC32 values keyed
 * by PK so {@link app.l2nx.gs.db.sync.engine.EntitySyncTask} can advance the
 * snapshot per-PK once Kafka acks the publish.</p>
 */
public final class ChangeSet {

    private final LongSet created;
    private final LongSet updated;
    private final LongSet deleted;
    private final Long2IntMap currentScan;

    public ChangeSet(LongSet created, LongSet updated, LongSet deleted, Long2IntMap currentScan) {
        this.created = created;
        this.updated = updated;
        this.deleted = deleted;
        this.currentScan = currentScan;
    }

    public LongSet created() {
        return created;
    }

    public LongSet updated() {
        return updated;
    }

    public LongSet deleted() {
        return deleted;
    }

    public Long2IntMap currentScan() {
        return currentScan;
    }

    public boolean isEmpty() {
        return created.isEmpty() && updated.isEmpty() && deleted.isEmpty();
    }

    public int totalChanges() {
        return created.size() + updated.size() + deleted.size();
    }

    /**
     * Compute the diff between the just-read window scan and the previous
     * snapshot for the same PK range.
     *
     * @param currentScan     Phase-1 result: PK → CRC32 for this window
     * @param prevKeysInRange PKs that were in the snapshot for this window's
     *                        range at the start of the cycle
     * @param snapshot        the snapshot store (used to read previous CRC32 for
     *                        "updated" detection)
     * @param entityName      entity scope key for snapshot lookup
     */
    public static ChangeSet diff(Long2IntMap currentScan,
                                 LongSet prevKeysInRange,
                                 SnapshotStore snapshot,
                                 String entityName) {
        LongOpenHashSet created = new LongOpenHashSet();
        LongOpenHashSet updated = new LongOpenHashSet();
        LongOpenHashSet deleted = new LongOpenHashSet();

        for (Long2IntMap.Entry e : currentScan.long2IntEntrySet()) {
            long pk = e.getLongKey();
            int newCrc = e.getIntValue();
            if (snapshot.containsCrc(entityName, pk)) {
                int prevCrc = snapshot.getCrc(entityName, pk);
                if (prevCrc != newCrc) {
                    updated.add(pk);
                }
            } else {
                created.add(pk);
            }
        }

        LongIterator prevIt = prevKeysInRange.iterator();
        while (prevIt.hasNext()) {
            long pk = prevIt.nextLong();
            if (!currentScan.containsKey(pk)) {
                deleted.add(pk);
            }
        }
        Long2IntOpenHashMap copy = currentScan instanceof Long2IntOpenHashMap
                ? (Long2IntOpenHashMap) currentScan
                : new Long2IntOpenHashMap(currentScan);
        return new ChangeSet(created, updated, deleted, copy);
    }
}
