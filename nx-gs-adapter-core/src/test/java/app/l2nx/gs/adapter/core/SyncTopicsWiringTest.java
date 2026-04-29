package app.l2nx.gs.adapter.core;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.KafkaConfig;
import app.l2nx.gs.adapter.api.rest.Topics;
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
        Map<String, String> wireTopics = new HashMap<String, String>();
        wireTopics.put("clan", "bohpts.gs.sync.clans");
        wireTopics.put("character", "bohpts.gs.sync.characters");

        NxAdapter.simulateInitKafkaForTesting(
                new KafkaInitializer(new CapturingKafkaFactory()),
                response(wireTopics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx, "module.onConnect was not invoked");
        assertEquals(wireTopics, ctx.getSyncTopics());
        assertTrue(CapturingAdapterModule.wasStarted(),
                "module.start should fire after a successful onConnect");
    }

    @Test
    void initKafka_shouldNormalizeNullSyncTopics_toEmptyMap() {
        NxAdapter.simulateInitKafkaForTesting(
                new KafkaInitializer(new CapturingKafkaFactory()),
                response(null));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx);
        assertNotNull(ctx.getSyncTopics(),
                "ConnectContext normalizes wire-null syncTopics to empty map");
        assertTrue(ctx.getSyncTopics().isEmpty());
    }

    @Test
    void initKafka_shouldExposeUnmodifiableSyncTopics_inConnectContext() {
        Map<String, String> wireTopics = new HashMap<String, String>();
        wireTopics.put("clan", "bohpts.gs.sync.clans");

        NxAdapter.simulateInitKafkaForTesting(
                new KafkaInitializer(new CapturingKafkaFactory()),
                response(wireTopics));

        ConnectContext ctx = CapturingAdapterModule.lastContext();
        assertNotNull(ctx);
        assertThrows(UnsupportedOperationException.class,
                () -> ctx.getSyncTopics().put("character", "x"));
    }

    private static ConnectResponse response(Map<String, String> syncTopics) {
        return ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("acme-x1")
                .serverName("Acme X1")
                .kafka(KafkaConfig.builder()
                        .bootstrap("k:9092")
                        .topics(new Topics("hb"))
                        .build())
                .syncTopics(syncTopics)
                .build();
    }
}
