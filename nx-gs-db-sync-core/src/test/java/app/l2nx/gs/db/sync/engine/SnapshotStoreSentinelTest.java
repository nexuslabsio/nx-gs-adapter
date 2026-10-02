package app.l2nx.gs.db.sync.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SnapshotStoreSentinelTest {

    @Test
    void missingHashSentinel_shouldBeIntegerMinValue() {
        assertEquals(
                Integer.MIN_VALUE,
                Phase1Hasher.MISSING_HASH,
                "MISSING_HASH must be Integer.MIN_VALUE to disambiguate from legit CRC32=0");
    }

    @Test
    void snapshotStore_defaultReturnValue_shouldBeMissingHashSentinel() {
        SnapshotStore store = new SnapshotStore();
        assertEquals(Phase1Hasher.MISSING_HASH, store.getCrc("nope", 1L));

        store.putCrc("clan", 1L, 0);
        store.removeCrc("clan", 1L);
        assertEquals(Phase1Hasher.MISSING_HASH, store.getCrc("clan", 1L));
    }

    @Test
    void getCrc_shouldReturnSentinel_whenPkAbsent() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 100);

        assertEquals(Phase1Hasher.MISSING_HASH, store.getCrc("clan", 2L));
    }

    @Test
    void putCrc_shouldAllowZero_andRoundTripWithoutCollidingWithSentinel() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 0);

        assertTrue(store.containsCrc("clan", 1L));
        assertEquals(0, store.getCrc("clan", 1L));
    }

    @Test
    void minPkMaxPk_shouldUseLazyMemoization_andRefreshAfterRemovingExtreme() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 5L, 50);
        store.putCrc("clan", 1L, 10);
        store.putCrc("clan", 100L, 1000);
        store.putCrc("clan", 20L, 200);

        assertEquals(1L, store.minPk("clan").getAsLong());
        assertEquals(100L, store.maxPk("clan").getAsLong());

        store.removeCrc("clan", 100L);
        assertEquals(20L, store.maxPk("clan").getAsLong());
        assertEquals(1L, store.minPk("clan").getAsLong());

        store.removeCrc("clan", 1L);
        assertEquals(5L, store.minPk("clan").getAsLong());
    }

    @Test
    void bucketByWindows_shouldReturnPerWindowKeySetsInSinglePass() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 10);
        store.putCrc("clan", 5L, 50);
        store.putCrc("clan", 10L, 100);
        store.putCrc("clan", 15L, 150);
        store.putCrc("clan", 20L, 200);

        Long2ObjectOpenHashMap<LongSet> buckets = store.bucketByWindows(
                "clan", Arrays.asList(new Window(0L, 5L), new Window(6L, 15L), new Window(16L, 25L)));

        assertEquals(3, buckets.size());
        assertEquals(2, buckets.get(0L).size());
        assertTrue(buckets.get(0L).contains(1L));
        assertTrue(buckets.get(0L).contains(5L));
        assertEquals(2, buckets.get(1L).size());
        assertTrue(buckets.get(1L).contains(10L));
        assertTrue(buckets.get(1L).contains(15L));
        assertEquals(1, buckets.get(2L).size());
        assertTrue(buckets.get(2L).contains(20L));
    }
}
