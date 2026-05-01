package app.l2nx.gs.adapter.core;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.KafkaConfig;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.core.kafka.CapturingKafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.adapter.core.modules.CapturingAdapterModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@code syncTopics} in a {@code ConnectResponse} survives the
 * adapter-core handshake plumbing and arrives at a registered
 * {@code AdapterModule.onConnect(ctx)} via {@code ctx.syncTopics()}.
 *
 * <p>Discovery is exercised through the real {@link java.util.ServiceLoader} path —
 * {@link CapturingAdapterModule} is registered via
 * {@code src/test/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule}.</p>
 */
class SyncTopicsWiringTest {

    @BeforeEach
    void setUp() {
        NxAdapter.resetForTesting();
        CapturingAdapterModule.reset();
        NxAdapter.primeModuleRegistryForTesting();
        // Sanity check: assert no leaked context from a prior test that exercised
        // ServiceLoader against the global META-INF registration. If this trips,
        // CapturingAdapterModule is being driven from a test that didn't reset() it.
        assertNull(CapturingAdapterModule.lastContext(),
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
        SyncTopics topics = SyncTopics.builder().db(dbTopics).runtime(runtimeTopics).build();

        NxAdapter.simulateInitKafkaForTesting(
                new KafkaInitializer(new CapturingKafkaFactory()),
                response(topics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx, "module.onConnect was not invoked");
        assertEquals(dbTopics, ctx.getSyncTopics().getDb());
        assertEquals(runtimeTopics, ctx.getSyncTopics().getRuntime());
        assertTrue(ctx.getSyncTopics().getDp().isEmpty());
        assertTrue(CapturingAdapterModule.wasStarted(),
                "module.start should fire after a successful onConnect");
    }

    @Test
    void initKafka_shouldNormalizeNullSyncTopics_toEmptyNamespaces() {
        NxAdapter.simulateInitKafkaForTesting(
                new KafkaInitializer(new CapturingKafkaFactory()),
                response(null));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx);
        assertNotNull(ctx.getSyncTopics(),
                "ConnectContext normalizes wire-null syncTopics to empty SyncTopics");
        assertTrue(ctx.getSyncTopics().getDb().isEmpty());
        assertTrue(ctx.getSyncTopics().getRuntime().isEmpty());
        assertTrue(ctx.getSyncTopics().getDp().isEmpty());
    }

    @Test
    void initKafka_shouldExposeUnmodifiableNamespaces_inConnectContext() {
        SyncTopics topics = SyncTopics.builder()
                .db(java.util.Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .build();

        NxAdapter.simulateInitKafkaForTesting(
                new KafkaInitializer(new CapturingKafkaFactory()),
                response(topics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx);
        assertThrows(UnsupportedOperationException.class,
                () -> ctx.getSyncTopics().getDb().put("character", "x"));
    }

    private static ConnectResponse response(SyncTopics syncTopics) {
        return ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("acme-x1")
                .serverName("Acme X1")
                .kafka(KafkaConfig.builder().bootstrap("k:9092").build())
                .heartbeatTopic("acme.gs.heartbeat")
                .syncTopics(syncTopics)
                .build();
    }
}
