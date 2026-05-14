package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChangeSetTest {

    private static final String ENTITY = "clan";

    @Test
    void diff_shouldClassifyEverythingAsCreated_whenSnapshotEmpty() {
        Long2IntMap scan = new Long2IntOpenHashMap();
        scan.put(1L, 100);
        scan.put(2L, 200);
        scan.put(3L, 300);

        ChangeSet diff = ChangeSet.diff(scan, new LongOpenHashSet(), new SnapshotStore(), ENTITY);

        assertEquals(3, diff.created().size());
        assertTrue(diff.updated().isEmpty());
        assertTrue(diff.deleted().isEmpty());
    }

    @Test
    void diff_shouldDetectUpdated_whenCrcDiffersForSamePk() {
        Long2IntMap scan = new Long2IntOpenHashMap();
        scan.put(1L, 200);

        SnapshotStore snapshot = new SnapshotStore();
        snapshot.putCrc(ENTITY, 1L, 100);

        LongSet prev = new LongOpenHashSet();
        prev.add(1L);

        ChangeSet diff = ChangeSet.diff(scan, prev, snapshot, ENTITY);

        assertTrue(diff.created().isEmpty());
        assertEquals(1, diff.updated().size());
        assertTrue(diff.updated().contains(1L));
        assertTrue(diff.deleted().isEmpty());
    }

    @Test
    void diff_shouldNotMarkUpdated_whenCrcUnchanged() {
        Long2IntMap scan = new Long2IntOpenHashMap();
        scan.put(1L, 100);

        SnapshotStore snapshot = new SnapshotStore();
        snapshot.putCrc(ENTITY, 1L, 100);

        LongSet prev = new LongOpenHashSet();
        prev.add(1L);

        ChangeSet diff = ChangeSet.diff(scan, prev, snapshot, ENTITY);

        assertTrue(diff.isEmpty());
    }

    @Test
    void diff_shouldDetectDeleted_whenPkInSnapshotRangeButMissingFromScan() {
        Long2IntMap scan = new Long2IntOpenHashMap();
        scan.put(1L, 100);

        SnapshotStore snapshot = new SnapshotStore();
        snapshot.putCrc(ENTITY, 1L, 100);
        snapshot.putCrc(ENTITY, 2L, 200);

        LongSet prev = new LongOpenHashSet();
        prev.add(1L);
        prev.add(2L);

        ChangeSet diff = ChangeSet.diff(scan, prev, snapshot, ENTITY);

        assertTrue(diff.created().isEmpty());
        assertTrue(diff.updated().isEmpty());
        assertEquals(1, diff.deleted().size());
        assertTrue(diff.deleted().contains(2L));
    }

    @Test
    void diff_shouldHandleMixedChanges() {
        Long2IntMap scan = new Long2IntOpenHashMap();
        scan.put(1L, 100);  // unchanged
        scan.put(2L, 250);  // updated (was 200)
        scan.put(4L, 400);  // created

        SnapshotStore snapshot = new SnapshotStore();
        snapshot.putCrc(ENTITY, 1L, 100);
        snapshot.putCrc(ENTITY, 2L, 200);
        snapshot.putCrc(ENTITY, 3L, 300); // deleted

        LongSet prev = new LongOpenHashSet();
        prev.add(1L);
        prev.add(2L);
        prev.add(3L);

        ChangeSet diff = ChangeSet.diff(scan, prev, snapshot, ENTITY);

        assertEquals(1, diff.created().size());
        assertTrue(diff.created().contains(4L));
        assertEquals(1, diff.updated().size());
        assertTrue(diff.updated().contains(2L));
        assertEquals(1, diff.deleted().size());
        assertTrue(diff.deleted().contains(3L));
        assertEquals(3, diff.totalChanges());
        assertFalse(diff.isEmpty());
    }

}
