package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Diff result of a single window's Phase-1 scan: PKs partitioned into
 * created (new in this scan), updated (CRC32 differs from snapshot), and
 * deleted (in snapshot for this window's PK range, missing in current scan).
 */
public final class ChangeSet {

    private final LongSet created;
    private final LongSet updated;
    private final LongSet deleted;

    public ChangeSet(LongSet created, LongSet updated, LongSet deleted) {
        this.created = created;
        this.updated = updated;
        this.deleted = deleted;
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
    public static ChangeSet diff(
            Long2IntMap currentScan, LongSet prevKeysInRange, SnapshotStore snapshot, String entityName) {
        LongOpenHashSet created = new LongOpenHashSet();
        LongOpenHashSet updated = new LongOpenHashSet();
        LongOpenHashSet deleted = new LongOpenHashSet();

        for (Long2IntMap.Entry e : currentScan.long2IntEntrySet()) {
            long pk = e.getLongKey();
            int newCrc = e.getIntValue();
            // containsCrc + getCrc instead of comparing getCrc against MISSING_HASH:
            // a real CRC32 value can collide with the sentinel (Integer.MIN_VALUE),
            // which would otherwise misclassify tracked rows as CREATED on every cycle.
            if (!snapshot.containsCrc(entityName, pk)) {
                created.add(pk);
            } else if (snapshot.getCrc(entityName, pk) != newCrc) {
                updated.add(pk);
            }
        }

        LongIterator prevIt = prevKeysInRange.iterator();
        while (prevIt.hasNext()) {
            long pk = prevIt.nextLong();
            if (!currentScan.containsKey(pk)) {
                deleted.add(pk);
            }
        }
        return new ChangeSet(created, updated, deleted);
    }
}
