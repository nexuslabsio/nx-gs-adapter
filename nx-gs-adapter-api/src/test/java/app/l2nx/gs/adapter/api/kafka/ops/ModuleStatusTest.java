package app.l2nx.gs.adapter.api.kafka.ops;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ModuleStatusTest {

    @Test
    void empty_shouldBeSingleton() {
        assertSame(ModuleStatus.Stats.empty(), ModuleStatus.Stats.empty());
    }

    @Test
    void empty_shouldHaveNoSlots() {
        assertFalse(ModuleStatus.Stats.empty().getPool().isPresent());
    }

    @Test
    void stats_shouldExposePool() {
        PoolStats pool = new PoolStats(1, 3, 4);
        ModuleStatus.Stats stats = ModuleStatus.Stats.builder().pool(pool).build();

        assertEquals(Optional.of(pool), stats.getPool());
    }

    @Test
    void constructor_shouldDefaultStatsToEmpty_whenNullPassed() {
        ModuleStatus status = new ModuleStatus("db-sync", "ACTIVE", null);

        assertSame(ModuleStatus.Stats.empty(), status.getStats());
    }

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        PoolStats pool = new PoolStats(1, 3, 4);
        ModuleStatus status = ModuleStatus.builder()
                .name("db-sync")
                .state("ACTIVE")
                .stats(ModuleStatus.Stats.builder().pool(pool).build())
                .build();

        assertEquals("db-sync", status.getName());
        assertEquals("ACTIVE", status.getState());
        assertTrue(status.getStats().getPool().isPresent());
        assertEquals(pool, status.getStats().getPool().get());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ModuleStatus original = ModuleStatus.builder()
                .name("db-sync")
                .state("DEGRADED")
                .stats(ModuleStatus.Stats.builder().pool(new PoolStats(0, 4, 4)).build())
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
