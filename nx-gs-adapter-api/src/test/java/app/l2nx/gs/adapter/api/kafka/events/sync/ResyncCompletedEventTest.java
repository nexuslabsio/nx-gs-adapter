package app.l2nx.gs.adapter.api.kafka.events.sync;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ResyncCompletedEventTest {

    private static final UUID EVENT_ID = UUID.fromString("018f0000-0000-7000-8000-00000000000a");
    private static final UUID RESYNC_ID = UUID.fromString("018f0000-0000-7000-8000-00000000000b");
    private static final Instant STARTED = Instant.parse("2026-06-12T10:00:00Z");
    private static final Instant COMPLETED = Instant.parse("2026-06-12T10:05:00Z");

    @Test
    void builder_shouldRoundtripFields() {
        ResyncCompletedEvent event = ResyncCompletedEvent.builder()
                .eventId(EVENT_ID)
                .resyncId(RESYNC_ID)
                .entityName("item")
                .cycleStartedAt(STARTED)
                .completedAt(COMPLETED)
                .build();

        assertEquals(EVENT_ID, event.getEventId());
        assertEquals(RESYNC_ID, event.getResyncId());
        assertEquals("item", event.getEntityName());
        assertEquals(STARTED, event.getCycleStartedAt());
        assertEquals(COMPLETED, event.getCompletedAt());
    }

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(
                NullPointerException.class,
                () -> new ResyncCompletedEvent(null, RESYNC_ID, "item", STARTED, COMPLETED));
    }

    @Test
    void constructor_shouldRejectNullResyncId() {
        assertThrows(
                NullPointerException.class, () -> new ResyncCompletedEvent(EVENT_ID, null, "item", STARTED, COMPLETED));
    }

    @Test
    void constructor_shouldRejectNullEntityName() {
        assertThrows(
                NullPointerException.class,
                () -> new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, null, STARTED, COMPLETED));
    }

    @Test
    void constructor_shouldRejectNullCycleStartedAt() {
        assertThrows(
                NullPointerException.class,
                () -> new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "item", null, COMPLETED));
    }

    @Test
    void constructor_shouldRejectNullCompletedAt() {
        assertThrows(
                NullPointerException.class, () -> new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "item", STARTED, null));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ResyncCompletedEvent original = ResyncCompletedEvent.builder()
                .eventId(EVENT_ID)
                .resyncId(RESYNC_ID)
                .entityName("clan")
                .cycleStartedAt(STARTED)
                .completedAt(COMPLETED)
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishOnEntityName() {
        ResyncCompletedEvent a = new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "clan", STARTED, COMPLETED);
        ResyncCompletedEvent b = new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "item", STARTED, COMPLETED);

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        ResyncCompletedEvent a = new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "clan", STARTED, COMPLETED);
        ResyncCompletedEvent b = new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "clan", STARTED, COMPLETED);

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeIdentityFields() {
        String s = new ResyncCompletedEvent(EVENT_ID, RESYNC_ID, "clan", STARTED, COMPLETED).toString();

        assertTrue(s.contains(RESYNC_ID.toString()));
        assertTrue(s.contains("clan"));
    }
}
