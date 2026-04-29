package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EntityStatsTrackerTest {

    @Test
    void currentStatuses_shouldBeEmpty_beforeFirstCycle() {
        EntityStatsTracker tracker = new EntityStatsTracker();

        assertTrue(tracker.currentStatuses().isEmpty());
    }

    @Test
    void recordCycleResult_shouldExposeLatestStats() {
        EntityStatsTracker tracker = new EntityStatsTracker();
        CycleResult cycle = new CycleResult(EntityState.HEALTHY, 100L, 5L, 2L, 1L, 100L);

        tracker.recordCycleResult("clan", cycle);

        List<EntityStats> snapshot = tracker.currentStatuses();
        assertEquals(1, snapshot.size());
        EntityStats stats = snapshot.get(0);
        assertEquals("clan", stats.getName());
        assertEquals(EntityState.HEALTHY, stats.getState());
        assertEquals(Long.valueOf(100L), stats.getRowCount());
        assertEquals(Long.valueOf(100L), stats.getLastCycleDurationMs());
        assertNotNull(stats.getLastCycleChanges());
        assertEquals(5L, stats.getLastCycleChanges().getCreated());
        assertEquals(2L, stats.getLastCycleChanges().getUpdated());
        assertEquals(1L, stats.getLastCycleChanges().getDeleted());
        assertEquals(Integer.valueOf(0), stats.getConsecutiveErrors());
    }

    @Test
    void consecutiveErrors_shouldIncrementOnDegraded_resetOnHealthy() {
        EntityStatsTracker tracker = new EntityStatsTracker();

        tracker.recordCycleResult("clan", CycleResult.degraded(50L));
        assertEquals(1, tracker.consecutiveErrors("clan"));

        tracker.recordCycleResult("clan", CycleResult.degraded(50L));
        assertEquals(2, tracker.consecutiveErrors("clan"));

        tracker.recordCycleResult("clan", CycleResult.degraded(50L));
        assertEquals(3, tracker.consecutiveErrors("clan"));

        tracker.recordCycleResult("clan",
                new CycleResult(EntityState.HEALTHY, 50L, 0L, 0L, 0L, 100L));
        assertEquals(0, tracker.consecutiveErrors("clan"));
    }

    @Test
    void currentStatuses_shouldPreserveInsertionOrder_acrossEntities() {
        EntityStatsTracker tracker = new EntityStatsTracker();

        tracker.recordCycleResult("clan",
                new CycleResult(EntityState.HEALTHY, 50L, 0L, 0L, 0L, 100L));
        tracker.recordCycleResult("character",
                new CycleResult(EntityState.HEALTHY, 50L, 0L, 0L, 0L, 200L));
        tracker.recordCycleResult("item",
                new CycleResult(EntityState.HEALTHY, 50L, 0L, 0L, 0L, 300L));

        List<EntityStats> snapshot = tracker.currentStatuses();

        assertEquals(3, snapshot.size());
        assertEquals("clan", snapshot.get(0).getName());
        assertEquals("character", snapshot.get(1).getName());
        assertEquals("item", snapshot.get(2).getName());
    }

    @Test
    void currentStatuses_shouldOverwriteEntityRow_onSubsequentCycle() {
        EntityStatsTracker tracker = new EntityStatsTracker();

        tracker.recordCycleResult("clan",
                new CycleResult(EntityState.HEALTHY, 50L, 1L, 0L, 0L, 100L));
        tracker.recordCycleResult("clan",
                new CycleResult(EntityState.HEALTHY, 60L, 0L, 5L, 1L, 105L));

        List<EntityStats> snapshot = tracker.currentStatuses();

        assertEquals(1, snapshot.size());
        EntityStats stats = snapshot.get(0);
        assertEquals(Long.valueOf(60L), stats.getLastCycleDurationMs());
        assertEquals(Long.valueOf(105L), stats.getRowCount());
        assertEquals(0L, stats.getLastCycleChanges().getCreated());
        assertEquals(5L, stats.getLastCycleChanges().getUpdated());
    }

    @Test
    void currentStatuses_shouldReturnUnmodifiableList() {
        EntityStatsTracker tracker = new EntityStatsTracker();
        tracker.recordCycleResult("clan",
                new CycleResult(EntityState.HEALTHY, 50L, 0L, 0L, 0L, 100L));

        List<EntityStats> snapshot = tracker.currentStatuses();

        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(null));
    }

    @Test
    void clear_shouldResetEverything() {
        EntityStatsTracker tracker = new EntityStatsTracker();
        tracker.recordCycleResult("clan", CycleResult.degraded(50L));
        tracker.recordCycleResult("clan", CycleResult.degraded(50L));

        tracker.clear();

        assertTrue(tracker.currentStatuses().isEmpty());
        assertEquals(0, tracker.consecutiveErrors("clan"));
    }
}
