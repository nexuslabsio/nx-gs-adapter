package app.l2nx.gs.adapter.api.kafka.ops;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ModuleStatusTest {

    @Test
    void empty_shouldBeSingleton() {
        assertSame(ModuleStatus.Stats.empty(), ModuleStatus.Stats.empty());
    }

    @Test
    void empty_shouldHaveNoSlots() {
        assertFalse(ModuleStatus.Stats.empty().getPool().isPresent());
        assertFalse(ModuleStatus.Stats.empty().getEntities().isPresent());
    }

    @Test
    void stats_shouldExposePool() {
        PoolStats pool = new PoolStats(1, 3, 4, null);
        ModuleStatus.Stats stats = ModuleStatus.Stats.builder().pool(pool).build();

        assertEquals(Optional.of(pool), stats.getPool());
    }

    @Test
    void stats_shouldExposeEntities() {
        EntityStats clan = EntityStats.builder()
                .name("clan")
                .state(EntityState.HEALTHY)
                .rowCount(1069L)
                .build();
        EntityStats character = EntityStats.builder()
                .name("character")
                .state(EntityState.DEGRADED)
                .rowCount(152_088L)
                .build();

        ModuleStatus.Stats stats = ModuleStatus.Stats.builder()
                .entities(Arrays.asList(clan, character))
                .build();

        assertTrue(stats.getEntities().isPresent());
        assertEquals(Arrays.asList(clan, character), stats.getEntities().get());
    }

    @Test
    void entities_shouldBeUnmodifiable() {
        EntityStats clan =
                EntityStats.builder().name("clan").state(EntityState.HEALTHY).build();
        ModuleStatus.Stats stats = ModuleStatus.Stats.builder()
                .entities(Collections.singletonList(clan))
                .build();

        List<EntityStats> seen = stats.getEntities().get();
        assertThrows(UnsupportedOperationException.class, () -> seen.add(clan));
    }

    @Test
    void entities_shouldDefensivelyCopy_whenSourceMutates() {
        EntityStats a =
                EntityStats.builder().name("clan").state(EntityState.HEALTHY).build();
        EntityStats b = EntityStats.builder()
                .name("character")
                .state(EntityState.HEALTHY)
                .build();
        java.util.ArrayList<EntityStats> source = new java.util.ArrayList<EntityStats>();
        source.add(a);

        ModuleStatus.Stats stats = ModuleStatus.Stats.builder().entities(source).build();
        source.add(b);

        assertEquals(Collections.singletonList(a), stats.getEntities().get());
    }

    @Test
    void constructor_shouldDefaultStatsToEmpty_whenNullPassed() {
        ModuleStatus status = new ModuleStatus("db-sync", "ACTIVE", null);

        assertSame(ModuleStatus.Stats.empty(), status.getStats());
    }

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        PoolStats pool = new PoolStats(1, 3, 4, null);
        EntityStats clan =
                EntityStats.builder().name("clan").state(EntityState.HEALTHY).build();
        ModuleStatus status = ModuleStatus.builder()
                .name("db-sync")
                .state("ACTIVE")
                .stats(ModuleStatus.Stats.builder()
                        .pool(pool)
                        .entities(Collections.singletonList(clan))
                        .build())
                .build();

        assertEquals("db-sync", status.getName());
        assertEquals("ACTIVE", status.getState());
        assertEquals(Optional.of(pool), status.getStats().getPool());
        assertEquals(
                Optional.of(Collections.singletonList(clan)), status.getStats().getEntities());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        EntityStats clan = EntityStats.builder()
                .name("clan")
                .state(EntityState.HEALTHY)
                .rowCount(3L)
                .build();

        ModuleStatus original = ModuleStatus.builder()
                .name("db-sync")
                .state("DEGRADED")
                .stats(ModuleStatus.Stats.builder()
                        .pool(new PoolStats(0, 4, 4, null))
                        .entities(Collections.singletonList(clan))
                        .build())
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
