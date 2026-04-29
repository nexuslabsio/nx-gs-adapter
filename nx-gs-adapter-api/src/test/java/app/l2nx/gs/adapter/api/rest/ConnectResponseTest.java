package app.l2nx.gs.adapter.api.rest;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ConnectResponseTest {

    @Test
    void syncTopics_shouldDefaultToNull_whenBuilderOmitsIt() {
        ConnectResponse response = ConnectResponse.builder()
                .tenantId(UUID.randomUUID()).tenantSlug("acme")
                .serverId(UUID.randomUUID()).serverSlug("primary")
                .serverName("Acme Primary")
                .kafka(KafkaConfig.builder().bootstrap("localhost:9092").build())
                .build();

        assertNull(response.getSyncTopics());
    }

    @Test
    void syncTopics_shouldExposeMap_whenBuilderProvidesIt() {
        Map<String, String> topics = new HashMap<String, String>();
        topics.put("clan", "bohpts.gs.sync.clans");
        topics.put("character", "bohpts.gs.sync.characters");

        ConnectResponse response = ConnectResponse.builder()
                .syncTopics(topics)
                .build();

        assertEquals(topics, response.getSyncTopics());
    }

    @Test
    void syncTopics_shouldBeUnmodifiable() {
        Map<String, String> topics = new HashMap<String, String>();
        topics.put("clan", "bohpts.gs.sync.clans");

        ConnectResponse response = ConnectResponse.builder().syncTopics(topics).build();

        assertThrows(UnsupportedOperationException.class,
                () -> response.getSyncTopics().put("character", "x"));
    }

    @Test
    void syncTopics_shouldDefensivelyCopy_whenSourceMutates() {
        Map<String, String> source = new HashMap<String, String>();
        source.put("clan", "bohpts.gs.sync.clans");

        ConnectResponse response = ConnectResponse.builder().syncTopics(source).build();
        source.put("character", "should-not-leak");

        assertEquals(Collections.singletonMap("clan", "bohpts.gs.sync.clans"),
                response.getSyncTopics());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ConnectResponse original = ConnectResponse.builder()
                .tenantId(UUID.randomUUID()).tenantSlug("acme")
                .serverId(UUID.randomUUID()).serverSlug("primary")
                .serverName("Acme")
                .kafka(KafkaConfig.builder().bootstrap("localhost:9092").build())
                .syncTopics(Collections.singletonMap("clan", "bohpts.gs.sync.clans"))
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
