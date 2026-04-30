package app.l2nx.gs.db.sync.engine;

import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotStoreTest {

    @Test
    void getCrc_shouldReturnZero_whenEntityUnknown() {
        SnapshotStore store = new SnapshotStore();

        assertEquals(0, store.getCrc("clan", 1L));
        assertFalse(store.containsCrc("clan", 1L));
    }

    @Test
    void putCrc_thenGetCrc_shouldRoundTripValue() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 42L, 12345);

        assertEquals(12345, store.getCrc("clan", 42L));
        assertTrue(store.containsCrc("clan", 42L));
    }

    @Test
    void putCrc_shouldOverwriteExistingValue() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 42L, 100);
        store.putCrc("clan", 42L, 200);

        assertEquals(200, store.getCrc("clan", 42L));
    }

    @Test
    void removeCrc_shouldDropEntry() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 42L, 100);
        store.removeCrc("clan", 42L);

        assertFalse(store.containsCrc("clan", 42L));
    }

    @Test
    void keysInRange_shouldReturnEmpty_whenEntityUnknown() {
        SnapshotStore store = new SnapshotStore();

        LongSet keys = store.keysInRange("clan", 0L, 100L);
        assertTrue(keys.isEmpty());
    }

    @Test
    void keysInRange_shouldReturnOnlyKeysWithinClosedInterval() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 10);
        store.putCrc("clan", 5L, 50);
        store.putCrc("clan", 10L, 100);
        store.putCrc("clan", 15L, 150);
        store.putCrc("clan", 20L, 200);

        LongSet keys = store.keysInRange("clan", 5L, 15L);

        assertEquals(3, keys.size());
        assertTrue(keys.contains(5L));
        assertTrue(keys.contains(10L));
        assertTrue(keys.contains(15L));
    }

    @Test
    void keysInRange_shouldIsolateEntities() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 1);
        store.putCrc("character", 2L, 2);

        assertEquals(1, store.keysInRange("clan", 0L, 100L).size());
        assertEquals(1, store.keysInRange("character", 0L, 100L).size());
        assertTrue(store.keysInRange("item", 0L, 100L).isEmpty());
    }

    @Test
    void clearEntity_shouldRemoveOnlyTargetEntity() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 1);
        store.putCrc("character", 2L, 2);

        store.clearEntity("clan");

        assertEquals(0, store.sizeOf("clan"));
        assertEquals(1, store.sizeOf("character"));
    }

    @Test
    void clearAll_shouldDropEverything() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 1);
        store.putCrc("character", 2L, 2);

        store.clearAll();

        assertEquals(0, store.sizeOf("clan"));
        assertEquals(0, store.sizeOf("character"));
    }

    @Test
    void sizeOf_shouldReportEntryCountPerEntity() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 1);
        store.putCrc("clan", 2L, 2);
        store.putCrc("clan", 3L, 3);

        assertEquals(3, store.sizeOf("clan"));
        assertEquals(0, store.sizeOf("character"));
    }

    @Test
    void minPk_shouldReturnEmpty_whenEntityUnknown() {
        SnapshotStore store = new SnapshotStore();

        assertFalse(store.minPk("clan").isPresent());
    }

    @Test
    void minPk_shouldReturnEmpty_whenEntityClearedBackToZero() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 42L, 100);
        store.removeCrc("clan", 42L);

        assertFalse(store.minPk("clan").isPresent());
    }

    @Test
    void minPk_shouldReturnSmallestKey_whenPopulated() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 5L, 50);
        store.putCrc("clan", 1L, 10);
        store.putCrc("clan", 100L, 1000);
        store.putCrc("clan", 20L, 200);

        assertEquals(OptionalLong.of(1L), store.minPk("clan"));
    }

    @Test
    void maxPk_shouldReturnEmpty_whenEntityUnknown() {
        SnapshotStore store = new SnapshotStore();

        assertFalse(store.maxPk("clan").isPresent());
    }

    @Test
    void maxPk_shouldReturnLargestKey_whenPopulated() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 5L, 50);
        store.putCrc("clan", 1L, 10);
        store.putCrc("clan", 100L, 1000);
        store.putCrc("clan", 20L, 200);

        assertEquals(OptionalLong.of(100L), store.maxPk("clan"));
    }

    @Test
    void minPkAndMaxPk_shouldIsolateEntities() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 5L, 1);
        store.putCrc("character", 99L, 2);

        assertEquals(OptionalLong.of(5L), store.minPk("clan"));
        assertEquals(OptionalLong.of(5L), store.maxPk("clan"));
        assertEquals(OptionalLong.of(99L), store.minPk("character"));
        assertEquals(OptionalLong.of(99L), store.maxPk("character"));
        assertFalse(store.minPk("item").isPresent());
        assertFalse(store.maxPk("item").isPresent());
    }
}
