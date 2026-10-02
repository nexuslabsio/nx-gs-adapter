package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

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

    public static ChangeSet diff(
            Long2IntMap currentScan, LongSet prevKeysInRange, SnapshotStore snapshot, String entityName) {
        LongOpenHashSet created = new LongOpenHashSet();
        LongOpenHashSet updated = new LongOpenHashSet();
        LongOpenHashSet deleted = new LongOpenHashSet();

        for (Long2IntMap.Entry e : currentScan.long2IntEntrySet()) {
            long pk = e.getLongKey();
            int newCrc = e.getIntValue();
            // containsCrc + getCrc, not getCrc vs MISSING_HASH: a real CRC32 can equal the sentinel and would be
            // misread as CREATED every cycle.
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
