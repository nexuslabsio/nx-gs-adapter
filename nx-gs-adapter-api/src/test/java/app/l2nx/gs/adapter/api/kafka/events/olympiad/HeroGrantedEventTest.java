package app.l2nx.gs.adapter.api.kafka.events.olympiad;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class HeroGrantedEventTest {

    @Test
    void getClanId_shouldBeNullable_forOfflineWinnerOrNoClan() {
        HeroGrantedEvent event = newBuilder().clanId(null).build();

        assertNull(event.getClanId());
    }

    @Test
    void build_shouldThrowNpe_whenEventIdIsNull() {
        HeroGrantedEvent.Builder b = HeroGrantedEvent.builder()
                .charId(268437521L).classId(88).olympiadCycle(7);

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        HeroGrantedEvent original = HeroGrantedEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(268437521L)
                .classId(88)
                .clanId(101L)
                .olympiadCycle(7)
                .build();

        HeroGrantedEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishClassId() {
        HeroGrantedEvent.Builder base = newBuilder();

        assertNotEquals(base.classId(88).build(), base.classId(89).build());
    }

    @Test
    void equals_shouldDistinguishOlympiadCycle() {
        HeroGrantedEvent.Builder base = newBuilder();

        assertNotEquals(base.olympiadCycle(7).build(), base.olympiadCycle(8).build());
    }

    @Test
    void toString_shouldRenderCharIdAndClassId() {
        HeroGrantedEvent event = newBuilder().charId(268437521L).classId(88).build();

        String s = event.toString();
        assertTrue(s.contains("charId=268437521"));
        assertTrue(s.contains("classId=88"));
    }

    private static HeroGrantedEvent.Builder newBuilder() {
        return HeroGrantedEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(268437521L)
                .classId(88)
                .clanId(101L)
                .olympiadCycle(7);
    }
}
