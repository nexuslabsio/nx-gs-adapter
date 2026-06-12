package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotStoreInvalidateTest {

    @Test
    void invalidate_shouldPerturbStoredCrc_whenPkPresent() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 12345);

        store.invalidate("clan", 1L);

        int perturbed = store.getCrc("clan", 1L);
        assertNotEquals(12345, perturbed);
        assertNotEquals(Phase1Hasher.MISSING_HASH, perturbed);
    }

    @Test
    void invalidate_shouldNeverProduceMissingHash_whenFlipWouldHitSentinel() {
        // crc ^ 1 == MISSING_HASH exactly for this value — the corner the
        // secondary perturbation exists for.
        int corner = Phase1Hasher.MISSING_HASH ^ 1;
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, corner);

        store.invalidate("clan", 1L);

        int perturbed = store.getCrc("clan", 1L);
        assertNotEquals(corner, perturbed);
        assertNotEquals(Phase1Hasher.MISSING_HASH, perturbed);
    }

    @Test
    void invalidate_shouldInsertSentinelEntry_whenPkAbsent() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 100);

        store.invalidate("clan", 99L);

        assertTrue(store.containsCrc("clan", 99L));
        assertNotEquals(Phase1Hasher.MISSING_HASH, store.getCrc("clan", 99L));
    }

    @Test
    void invalidate_shouldInsertSentinelEntry_whenEntityUnknown() {
        SnapshotStore store = new SnapshotStore();

        store.invalidate("clan", 7L);

        assertTrue(store.containsCrc("clan", 7L));
        assertEquals(1, store.sizeOf("clan"));
    }

    @Test
    void invalidate_shouldExtendExtremes_whenSentinelInsertedBeyondOldMax() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 5L, 50);
        store.putCrc("clan", 10L, 100);
        // Materialize the extreme cache before the sentinel insert.
        assertEquals(5L, store.minPk("clan").getAsLong());
        assertEquals(10L, store.maxPk("clan").getAsLong());

        store.invalidate("clan", 100L);
        store.invalidate("clan", 1L);

        assertEquals(1L, store.minPk("clan").getAsLong());
        assertEquals(100L, store.maxPk("clan").getAsLong());
    }

    @Test
    void invalidateAll_shouldPerturbEveryEntry() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 1L, 0);
        store.putCrc("clan", 2L, 12345);
        store.putCrc("clan", 3L, Phase1Hasher.MISSING_HASH ^ 1);

        store.invalidateAll("clan");

        assertNotEquals(0, store.getCrc("clan", 1L));
        assertNotEquals(12345, store.getCrc("clan", 2L));
        assertNotEquals(Phase1Hasher.MISSING_HASH ^ 1, store.getCrc("clan", 3L));
        assertNotEquals(Phase1Hasher.MISSING_HASH, store.getCrc("clan", 1L));
        assertNotEquals(Phase1Hasher.MISSING_HASH, store.getCrc("clan", 2L));
        assertNotEquals(Phase1Hasher.MISSING_HASH, store.getCrc("clan", 3L));
        assertEquals(3, store.sizeOf("clan"));
    }

    @Test
    void invalidateAll_shouldKeepExtremesIntact() {
        SnapshotStore store = new SnapshotStore();
        store.putCrc("clan", 2L, 20);
        store.putCrc("clan", 40L, 400);
        assertEquals(2L, store.minPk("clan").getAsLong());

        store.invalidateAll("clan");

        assertEquals(2L, store.minPk("clan").getAsLong());
        assertEquals(40L, store.maxPk("clan").getAsLong());
    }

    @Test
    void invalidateAll_shouldBeNoOp_whenEntityUnknown() {
        SnapshotStore store = new SnapshotStore();

        store.invalidateAll("nope");

        assertEquals(0, store.sizeOf("nope"));
    }
}
