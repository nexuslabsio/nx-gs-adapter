package app.l2nx.gs.adapter.api.kafka.events.schedule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DayOfWeek;
import java.time.OffsetTime;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class RecurringSlotTest {

    @Test
    void builder_shouldBuildEqualToConstructor() {
        RecurringSlot built = RecurringSlot.builder()
                .daysOfWeek(EnumSet.of(DayOfWeek.TUESDAY))
                .time(OffsetTime.parse("19:00:00+03:00"))
                .jitterMinutes(30)
                .build();

        RecurringSlot ctor = new RecurringSlot(EnumSet.of(DayOfWeek.TUESDAY), OffsetTime.parse("19:00:00+03:00"), 30);

        assertEquals(ctor, built);
        assertEquals(ctor.hashCode(), built.hashCode());
    }

    @Test
    void daysOfWeek_shouldBeEmptySet_whenNull() {
        RecurringSlot slot = new RecurringSlot(null, OffsetTime.parse("12:00:00Z"), 0);

        assertNotNull(slot.getDaysOfWeek());
        assertTrue(slot.getDaysOfWeek().isEmpty());
    }

    @Test
    void daysOfWeek_shouldBeUnmodifiable() {
        RecurringSlot slot = new RecurringSlot(EnumSet.of(DayOfWeek.MONDAY), OffsetTime.parse("12:00:00Z"), 0);

        assertThrows(
                UnsupportedOperationException.class, () -> slot.getDaysOfWeek().add(DayOfWeek.SUNDAY));
    }

    @Test
    void daysOfWeek_shouldCopyDefensively() {
        EnumSet<DayOfWeek> source = EnumSet.of(DayOfWeek.MONDAY);
        RecurringSlot slot = new RecurringSlot(source, OffsetTime.parse("12:00:00Z"), 0);

        source.add(DayOfWeek.FRIDAY);

        assertEquals(EnumSet.of(DayOfWeek.MONDAY), slot.getDaysOfWeek());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        RecurringSlot original = new RecurringSlot(
                EnumSet.of(DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY), OffsetTime.parse("21:45:00+03:00"), 30);

        assertEquals(original, original.toBuilder().build());
    }
}
