package app.l2nx.gs.adapter.api.rest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ConnectResponseTest {

    @Test
    void syncTopics_shouldDefaultToNull_whenBuilderOmitsIt() {
        ConnectResponse response = ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme Primary")
                .kafka(KafkaCredentials.builder().bootstrap("localhost:9092").build())
                .build();

        assertNull(response.getSyncTopics());
        assertNull(response.getHeartbeatTopic());
    }

    @Test
    void heartbeatTopic_shouldRoundtrip() {
        ConnectResponse response =
                ConnectResponse.builder().heartbeatTopic("acme.gs.heartbeat").build();

        assertEquals("acme.gs.heartbeat", response.getHeartbeatTopic());
    }

    @Test
    void syncTopics_shouldExposeNamespaces_whenBuilderProvidesIt() {
        SyncTopics topics = SyncTopics.builder()
                .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .runtime(Collections.singletonMap("character", "bohpts.gs.sync.runtime.character"))
                .build();

        ConnectResponse response = ConnectResponse.builder().syncTopics(topics).build();

        assertEquals(topics, response.getSyncTopics());
        assertEquals("bohpts.gs.sync.db.clan", response.getSyncTopics().getDb().get("clan"));
        assertEquals(
                "bohpts.gs.sync.runtime.character",
                response.getSyncTopics().getRuntime().get("character"));
    }

    @Test
    void syncTopics_namespacesShouldBeUnmodifiable() {
        SyncTopics topics = SyncTopics.builder()
                .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .build();

        ConnectResponse response = ConnectResponse.builder().syncTopics(topics).build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> response.getSyncTopics().getDb().put("character", "x"));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ConnectResponse original = ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme")
                .kafka(KafkaCredentials.builder().bootstrap("localhost:9092").build())
                .heartbeatTopic("acme.gs.heartbeat")
                .syncTopics(SyncTopics.builder()
                        .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                        .build())
                .messagingTopics(MessagingTopics.builder()
                        .events(Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase"))
                        .build())
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void messagingTopics_shouldDefaultToNull_whenBuilderOmitsIt() {
        ConnectResponse response = ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme")
                .kafka(KafkaCredentials.builder().bootstrap("localhost:9092").build())
                .build();

        assertNull(response.getMessagingTopics());
    }

    @Test
    void messagingTopics_shouldExposeEventsMap_whenBuilderProvidesIt() {
        ConnectResponse response = ConnectResponse.builder()
                .messagingTopics(MessagingTopics.builder()
                        .events(Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase"))
                        .build())
                .build();

        assertEquals(
                "acme.gs.events.premiumpurchase",
                response.getMessagingTopics().getEvents().get("premiumpurchase"));
    }
}
