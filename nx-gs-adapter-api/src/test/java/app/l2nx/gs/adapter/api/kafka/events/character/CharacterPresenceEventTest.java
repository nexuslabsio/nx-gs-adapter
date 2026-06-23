package app.l2nx.gs.adapter.api.kafka.events.character;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class CharacterPresenceEventTest {

    private static UUID id() {
        return UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
    }

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(
                NullPointerException.class,
                () -> CharacterPresenceEvent.builder().charId(1L).online(true).build());
    }

    @Test
    void getMetadata_shouldReturnNull_whenBuilderOmits() {
        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(1L)
                .online(true)
                .build();

        assertNull(event.getMetadata());
    }

    @Test
    void getMetadata_shouldReturnNull_whenBuilderPassesNull() {
        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(1L)
                .online(false)
                .metadata(null)
                .build();

        assertNull(event.getMetadata());
    }

    @Test
    void getMetadata_shouldBeUnmodifiable() {
        Map<String, String> source = new HashMap<String, String>();
        source.put(WellKnownPresenceMetadata.LOGOUT_REASON, WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT);

        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(1L)
                .online(false)
                .metadata(source)
                .build();

        assertThrows(
                UnsupportedOperationException.class, () -> event.getMetadata().put("k", "v"));
    }

    @Test
    void constructor_shouldDefensivelyCopyMetadataMap() {
        Map<String, String> source = new HashMap<String, String>();
        source.put(WellKnownPresenceMetadata.LOGOUT_REASON, WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT);

        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(1L)
                .online(false)
                .metadata(source)
                .build();

        source.put("late", "value");

        assertEquals(1, event.getMetadata().size());
        assertEquals(
                WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT,
                event.getMetadata().get(WellKnownPresenceMetadata.LOGOUT_REASON));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<String, String> metadata = new LinkedHashMap<String, String>();
        metadata.put(WellKnownPresenceMetadata.LOGOUT_REASON, WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT);

        UUID sessionId = UUID.fromString("018f5fa3-1e3d-7000-8000-0000000000aa");
        CharacterPresenceEvent original = CharacterPresenceEvent.builder()
                .eventId(id())
                .charId(42L)
                .online(false)
                .sessionId(sessionId)
                .accountName("acc")
                .ip("1.2.3.4")
                .hwid("HW")
                .metadata(metadata)
                .build();

        CharacterPresenceEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
        assertEquals(sessionId, copy.getSessionId());
        assertEquals("acc", copy.getAccountName());
        assertEquals("1.2.3.4", copy.getIp());
        assertEquals("HW", copy.getHwid());
        assertFalse(copy.isOnline());
        assertEquals(42L, copy.getCharId());
    }

    @Test
    void getSessionId_shouldReturnNull_whenBuilderOmits() {
        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(id())
                .charId(1L)
                .online(true)
                .build();

        assertNull(event.getSessionId());
    }

    @Test
    void equals_shouldDistinguishSessionId() {
        UUID id = id();
        CharacterPresenceEvent withoutSession = CharacterPresenceEvent.builder()
                .eventId(id)
                .charId(1L)
                .online(true)
                .build();
        CharacterPresenceEvent withSession = CharacterPresenceEvent.builder()
                .eventId(id)
                .charId(1L)
                .online(true)
                .sessionId(UUID.fromString("018f5fa3-1e3d-7000-8000-0000000000bb"))
                .build();

        assertNotEquals(withoutSession, withSession);
    }

    @Test
    void equals_shouldDistinguishOnline() {
        UUID id = id();
        CharacterPresenceEvent login = CharacterPresenceEvent.builder()
                .eventId(id)
                .charId(1L)
                .online(true)
                .build();
        CharacterPresenceEvent logout = CharacterPresenceEvent.builder()
                .eventId(id)
                .charId(1L)
                .online(false)
                .build();

        assertNotEquals(login, logout);
    }

    @Test
    void equals_shouldDistinguishMetadata() {
        UUID id = id();
        CharacterPresenceEvent plain = CharacterPresenceEvent.builder()
                .eventId(id)
                .charId(1L)
                .online(false)
                .build();
        CharacterPresenceEvent disconnect = CharacterPresenceEvent.builder()
                .eventId(id)
                .charId(1L)
                .online(false)
                .metadata(Collections.singletonMap(
                        WellKnownPresenceMetadata.LOGOUT_REASON, WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT))
                .build();

        assertNotEquals(plain, disconnect);
    }

    @Test
    void toString_shouldRenderEventIdAndMetadata() {
        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(id())
                .charId(1L)
                .online(false)
                .metadata(Collections.singletonMap(
                        WellKnownPresenceMetadata.LOGOUT_REASON, WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT))
                .build();

        String s = event.toString();
        assertTrue(s.contains("eventId=" + id()));
        assertTrue(s.contains(WellKnownPresenceMetadata.LOGOUT_REASON));
        assertTrue(s.contains(WellKnownPresenceMetadata.LOGOUT_REASON_DISCONNECT));
    }
}
