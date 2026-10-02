package app.l2nx.gs.adapter.core;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.core.kafka.CapturingKafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.adapter.core.modules.CapturingAdapterModule;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SyncTopicsWiringTest {

    @BeforeEach
    void setUp() {
        NxAdapter.resetForTesting();
        CapturingAdapterModule.reset();
        NxAdapter.primeModuleRegistryForTesting();
        // Guards against a leaked CapturingAdapterModule context from a test that didn't reset()
        assertNull(
                CapturingAdapterModule.lastContext(),
                "CapturingAdapterModule leaked context from a prior test — reset() in @BeforeEach");
    }

    @AfterEach
    void tearDown() {
        NxAdapter.resetForTesting();
        CapturingAdapterModule.reset();
    }

    @Test
    void initKafka_shouldSurfaceSyncTopics_inConnectContext() {
        Map<String, String> dbTopics = new HashMap<String, String>();
        dbTopics.put("clan", "bohpts.gs.sync.db.clan");
        dbTopics.put("character", "bohpts.gs.sync.db.character");
        Map<String, String> runtimeTopics = new HashMap<String, String>();
        runtimeTopics.put("character", "bohpts.gs.sync.runtime.character");
        SyncTopics topics =
                SyncTopics.builder().db(dbTopics).runtime(runtimeTopics).build();

        NxAdapter.simulateInitKafkaForTesting(new KafkaInitializer(new CapturingKafkaFactory()), response(topics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx, "module.onConnect was not invoked");
        assertEquals(dbTopics, ctx.getSyncTopics().getDb());
        assertEquals(runtimeTopics, ctx.getSyncTopics().getRuntime());
        assertTrue(ctx.getSyncTopics().getGd().isEmpty());
        assertTrue(CapturingAdapterModule.wasStarted(), "module.start should fire after a successful onConnect");
    }

    @Test
    void initKafka_shouldNormalizeNullSyncTopics_toEmptyNamespaces() {
        NxAdapter.simulateInitKafkaForTesting(new KafkaInitializer(new CapturingKafkaFactory()), response(null));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx);
        assertNotNull(ctx.getSyncTopics(), "ConnectContext normalizes wire-null syncTopics to empty SyncTopics");
        assertTrue(ctx.getSyncTopics().getDb().isEmpty());
        assertTrue(ctx.getSyncTopics().getRuntime().isEmpty());
        assertTrue(ctx.getSyncTopics().getGd().isEmpty());
    }

    @Test
    void initKafka_shouldStillConnectSyncModule_whenEventsBootstrapThrows() {
        // Regression: an events-bootstrap failure (api/core version skew) must not abort sync-module discovery,
        // else heartbeat reports empty enabledModules and all sync silently stops
        NxAdapter.failEventsBootstrapForTesting(true);
        SyncTopics topics = SyncTopics.builder()
                .db(java.util.Collections.singletonMap("character", "bohpts.gs.sync.db.character"))
                .build();

        NxAdapter.simulateInitKafkaForTesting(new KafkaInitializer(new CapturingKafkaFactory()), response(topics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx, "sync module.onConnect must still fire when events bootstrap fails");
        assertTrue(
                CapturingAdapterModule.wasStarted(), "sync module.start must fire even though events bootstrap threw");
        assertEquals("bohpts.gs.sync.db.character", ctx.getSyncTopics().getDb().get("character"));
    }

    @Test
    void initKafka_shouldExposeUnmodifiableNamespaces_inConnectContext() {
        SyncTopics topics = SyncTopics.builder()
                .db(java.util.Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .build();

        NxAdapter.simulateInitKafkaForTesting(new KafkaInitializer(new CapturingKafkaFactory()), response(topics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx);
        assertThrows(
                UnsupportedOperationException.class,
                () -> ctx.getSyncTopics().getDb().put("character", "x"));
    }

    private static ConnectResponse response(SyncTopics syncTopics) {
        return ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("acme-x1")
                .serverName("Acme X1")
                .kafka(KafkaCredentials.builder().bootstrap("k:9092").build())
                .heartbeatTopic("acme.gs.heartbeat")
                .syncTopics(syncTopics)
                .build();
    }
}
