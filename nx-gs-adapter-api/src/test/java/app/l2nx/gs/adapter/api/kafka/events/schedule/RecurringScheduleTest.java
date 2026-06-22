package app.l2nx.gs.adapter.api.kafka.events.schedule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DayOfWeek;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class RecurringScheduleTest {

    private static RecurringSlot slot() {
        return new RecurringSlot(EnumSet.of(DayOfWeek.TUESDAY), OffsetTime.parse("19:00:00+03:00"), 30);
    }

    @Test
    void builder_shouldBuildEqualToConstructor() {
        RecurringSchedule built = RecurringSchedule.builder()
                .slots(Collections.singletonList(slot()))
                .build();

        RecurringSchedule ctor = new RecurringSchedule(Collections.singletonList(slot()));

        assertEquals(ctor, built);
        assertEquals(ctor.hashCode(), built.hashCode());
    }

    @Test
    void slots_shouldBeEmptyList_whenNull() {
        RecurringSchedule schedule = new RecurringSchedule(null);

        assertNotNull(schedule.getSlots());
        assertTrue(schedule.getSlots().isEmpty());
    }

    @Test
    void slots_shouldBeUnmodifiable() {
        RecurringSchedule schedule = new RecurringSchedule(Collections.singletonList(slot()));

        assertThrows(
                UnsupportedOperationException.class, () -> schedule.getSlots().add(slot()));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        RecurringSchedule original = new RecurringSchedule(Arrays.asList(slot(), slot()));

        assertEquals(original, original.toBuilder().build());
    }
}
