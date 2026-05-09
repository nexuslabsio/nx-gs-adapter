package app.l2nx.gs.adapter.api.rest;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessagingTopicsTest {

    @Test
    void getEvents_shouldReturnEmptyMap_whenConstructorReceivesNull() {
        MessagingTopics topics = new MessagingTopics(null, null, null);

        assertTrue(topics.getEvents().isEmpty());
    }

    @Test
    void getCommandsTopic_shouldReturnNull_whenConstructorReceivesNull() {
        MessagingTopics topics = new MessagingTopics(null, null, null);

        assertNull(topics.getCommandsTopic());
        assertNull(topics.getCommandsRepliesTopic());
    }

    @Test
    void getCommandsTopic_shouldReturnNull_whenConstructorReceivesBlank() {
        MessagingTopics topics = new MessagingTopics(null, "  ", "");

        assertNull(topics.getCommandsTopic());
        assertNull(topics.getCommandsRepliesTopic());
    }

    @Test
    void getEvents_shouldExposeProvidedKeys() {
        Map<String, String> events = new HashMap<String, String>();
        events.put("premiumpurchase", "acme.gs.events.premiumpurchase");
        events.put("character", "acme.gs.events.character");

        MessagingTopics topics = new MessagingTopics(events, null, null);

        assertEquals("acme.gs.events.premiumpurchase", topics.getEvents().get("premiumpurchase"));
        assertEquals("acme.gs.events.character", topics.getEvents().get("character"));
    }

    @Test
    void getEvents_shouldBeUnmodifiable() {
        MessagingTopics topics = MessagingTopics.builder()
                .events(Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase"))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> topics.getEvents().put("character", "x"));
    }

    @Test
    void commandsTopic_shouldRoundtrip() {
        MessagingTopics topics = MessagingTopics.builder()
                .commandsTopic("acme.gs.commands")
                .commandsRepliesTopic("acme.gs.commands.replies")
                .build();

        assertEquals("acme.gs.commands", topics.getCommandsTopic());
        assertEquals("acme.gs.commands.replies", topics.getCommandsRepliesTopic());
    }

    @Test
    void constructor_shouldDefensivelyCopySource_soLaterMutationsDontLeak() {
        Map<String, String> source = new HashMap<String, String>();
        source.put("premiumpurchase", "acme.gs.events.premiumpurchase");

        MessagingTopics topics = new MessagingTopics(source, null, null);
        source.put("character", "acme.gs.events.character"); // mutate after construction

        assertEquals(1, topics.getEvents().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        MessagingTopics original = MessagingTopics.builder()
                .events(Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase"))
                .commandsTopic("acme.gs.commands")
                .commandsRepliesTopic("acme.gs.commands.replies")
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishDifferentEventMaps() {
        MessagingTopics a = MessagingTopics.builder()
                .events(Collections.singletonMap("premiumpurchase", "a"))
                .build();
        MessagingTopics b = MessagingTopics.builder()
                .events(Collections.singletonMap("premiumpurchase", "b"))
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishDifferentCommandsTopic() {
        MessagingTopics a = MessagingTopics.builder().commandsTopic("a").build();
        MessagingTopics b = MessagingTopics.builder().commandsTopic("b").build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishDifferentCommandsRepliesTopic() {
        MessagingTopics a = MessagingTopics.builder().commandsRepliesTopic("a").build();
        MessagingTopics b = MessagingTopics.builder().commandsRepliesTopic("b").build();

        assertNotEquals(a, b);
    }
}
