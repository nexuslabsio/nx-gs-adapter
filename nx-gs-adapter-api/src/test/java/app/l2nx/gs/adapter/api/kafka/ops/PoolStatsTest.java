package app.l2nx.gs.adapter.api.kafka.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class PoolStatsTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        PoolStats stats =
                PoolStats.builder().active(2).idle(8).total(10).waiting(0).build();

        assertEquals(Integer.valueOf(2), stats.getActive());
        assertEquals(Integer.valueOf(8), stats.getIdle());
        assertEquals(Integer.valueOf(10), stats.getTotal());
        assertEquals(Integer.valueOf(0), stats.getWaiting());
    }

    @Test
    void allFields_shouldBeNullable_whenPoolDoesNotExposeThem() {
        PoolStats stats = PoolStats.builder().active(1).idle(0).build();

        assertEquals(Integer.valueOf(1), stats.getActive());
        assertEquals(Integer.valueOf(0), stats.getIdle());
        assertNull(stats.getTotal());
        assertNull(stats.getWaiting());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        PoolStats original = new PoolStats(3, 5, 8, 1);

        assertEquals(original, original.toBuilder().build());
    }
}
