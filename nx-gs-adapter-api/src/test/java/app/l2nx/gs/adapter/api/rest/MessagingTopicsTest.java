package app.l2nx.gs.adapter.api.rest;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessagingTopicsTest {

    @Test
    void getEvents_shouldReturnEmptyMap_whenConstructorReceivesNull() {
        MessagingTopics topics = new MessagingTopics(null, null);

        assertTrue(topics.getEvents().isEmpty());
    }

    @Test
    void getCommands_shouldReturnEmptyMap_whenConstructorReceivesNull() {
        MessagingTopics topics = new MessagingTopics(null, null);

        assertTrue(topics.getCommands().isEmpty());
    }

    @Test
    void getEvents_shouldExposeProvidedKeys() {
        Map<String, String> events = new HashMap<String, String>();
        events.put("premium", "acme.gs.events.premium");
        events.put("character", "acme.gs.events.character");

        MessagingTopics topics = new MessagingTopics(events, null);

        assertEquals("acme.gs.events.premium", topics.getEvents().get("premium"));
        assertEquals("acme.gs.events.character", topics.getEvents().get("character"));
    }

    @Test
    void getEvents_shouldBeUnmodifiable() {
        MessagingTopics topics = MessagingTopics.builder()
                .events(Collections.singletonMap("premium", "acme.gs.events.premium"))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> topics.getEvents().put("character", "x"));
    }

    @Test
    void getCommands_shouldBeUnmodifiable() {
        MessagingTopics topics = MessagingTopics.builder()
                .commands(Collections.singletonMap("char", "acme.gs.commands.char"))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> topics.getCommands().put("clan", "x"));
    }

    @Test
    void constructor_shouldDefensivelyCopySource_soLaterMutationsDontLeak() {
        Map<String, String> source = new HashMap<String, String>();
        source.put("premium", "acme.gs.events.premium");

        MessagingTopics topics = new MessagingTopics(source, null);
        source.put("character", "acme.gs.events.character"); // mutate after construction

        assertEquals(1, topics.getEvents().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        MessagingTopics original = MessagingTopics.builder()
                .events(Collections.singletonMap("premium", "acme.gs.events.premium"))
                .commands(Collections.singletonMap("char", "acme.gs.commands.char"))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishDifferentEventMaps() {
        MessagingTopics a = MessagingTopics.builder()
                .events(Collections.singletonMap("premium", "a"))
                .build();
        MessagingTopics b = MessagingTopics.builder()
                .events(Collections.singletonMap("premium", "b"))
                .build();

        assertNotEquals(a, b);
    }
}
