package app.l2nx.gs.adapter.api.kafka.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class EntityStatsTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        ChangesSummary changes =
                ChangesSummary.builder().created(2L).updated(1L).deleted(0L).build();

        EntityStats stats = EntityStats.builder()
                .name("clan")
                .state(EntityState.HEALTHY)
                .rowCount(1069L)
                .lastSyncEpochMs(1_700_000_000_000L)
                .lastCycleDurationMs(420L)
                .lastCycleChanges(changes)
                .consecutiveErrors(0)
                .build();

        assertEquals("clan", stats.getName());
        assertEquals(EntityState.HEALTHY, stats.getState());
        assertEquals(Long.valueOf(1069L), stats.getRowCount());
        assertEquals(Long.valueOf(1_700_000_000_000L), stats.getLastSyncEpochMs());
        assertEquals(Long.valueOf(420L), stats.getLastCycleDurationMs());
        assertEquals(changes, stats.getLastCycleChanges());
        assertEquals(Integer.valueOf(0), stats.getConsecutiveErrors());
    }

    @Test
    void countersAndChanges_shouldBeNullable_whenEngineHasNoCycleYet() {
        EntityStats stats = EntityStats.builder()
                .name("character")
                .state(EntityState.DEGRADED)
                .consecutiveErrors(1)
                .build();

        assertNull(stats.getRowCount());
        assertNull(stats.getLastSyncEpochMs());
        assertNull(stats.getLastCycleDurationMs());
        assertNull(stats.getLastCycleChanges());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        EntityStats original = EntityStats.builder()
                .name("clan")
                .state(EntityState.HEALTHY)
                .rowCount(3L)
                .lastSyncEpochMs(123L)
                .lastCycleDurationMs(45L)
                .lastCycleChanges(new ChangesSummary(1L, 1L, 1L))
                .consecutiveErrors(0)
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
