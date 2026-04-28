package app.l2nx.gs.adapter.api.kafka.ops;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PoolStatsTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        PoolStats stats = PoolStats.builder().busy(2).idle(8).total(10).build();

        assertEquals(2, stats.getBusy());
        assertEquals(8, stats.getIdle());
        assertEquals(Integer.valueOf(10), stats.getTotal());
    }

    @Test
    void total_shouldBeNullable_whenPoolDoesNotExposeIt() {
        PoolStats stats = PoolStats.builder().busy(1).idle(0).total(null).build();

        assertNull(stats.getTotal());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        PoolStats original = new PoolStats(3, 5, 8);

        assertEquals(original, original.toBuilder().build());
    }
}
