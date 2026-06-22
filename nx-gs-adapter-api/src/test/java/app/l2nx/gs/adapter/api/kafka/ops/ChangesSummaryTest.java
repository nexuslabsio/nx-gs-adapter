package app.l2nx.gs.adapter.api.kafka.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ChangesSummaryTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        ChangesSummary summary =
                ChangesSummary.builder().created(3L).updated(7L).deleted(1L).build();

        assertEquals(3L, summary.getCreated());
        assertEquals(7L, summary.getUpdated());
        assertEquals(1L, summary.getDeleted());
    }

    @Test
    void builder_shouldDefaultEachFieldToZero_whenOmitted() {
        ChangesSummary summary = ChangesSummary.builder().build();

        assertEquals(0L, summary.getCreated());
        assertEquals(0L, summary.getUpdated());
        assertEquals(0L, summary.getDeleted());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ChangesSummary original = new ChangesSummary(2L, 5L, 0L);

        assertEquals(original, original.toBuilder().build());
    }
}
