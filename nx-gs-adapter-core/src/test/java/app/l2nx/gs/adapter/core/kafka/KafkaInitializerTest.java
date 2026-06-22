package app.l2nx.gs.adapter.core.kafka;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.kafka.KafkaState;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class KafkaInitializerTest {

    private static final UUID SERVER_ID = UUID.fromString("01997e26-1000-7000-8000-000000000001");

    private static KafkaCredentials kafkaCredentials(String user, String password) {
        return KafkaCredentials.builder()
                .bootstrap("kafka.l2nx.app:9092")
                .securityProtocol("SASL_SSL")
                .saslMechanism("SCRAM-SHA-256")
                .saslUsername(user)
                .saslPassword(password)
                .build();
    }

    @Test
    void init_shouldComposePropertiesAndForwardListenerAndHeaders() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        KafkaInitializer init = new KafkaInitializer(factory);
        Consumer<KafkaState> listener = state -> {
            /* test placeholder */
        };
        Map<String, byte[]> staticHeaders =
                Collections.singletonMap(NxHeaders.NX_SERVER_ID, NxHeaders.encodeUuid(SERVER_ID));

        KafkaState result = init.init(
                kafkaCredentials("acme-x1-user", "p@ss"), "nx-gs-adapter-acme-acme-x1", staticHeaders, listener);

        assertEquals(KafkaState.CONNECTED, result);
        assertEquals(1, factory.callCount);
        assertEquals("kafka.l2nx.app:9092", factory.capturedBrokers);
        assertEquals("nx-gs-adapter-acme-acme-x1", factory.capturedClientId);
        assertSame(listener, factory.capturedListener);
        assertNotNull(factory.capturedProperties);
        assertEquals("SASL_SSL", factory.capturedProperties.get("security.protocol"));
        assertEquals("SCRAM-SHA-256", factory.capturedProperties.get("sasl.mechanism"));
        assertEquals(
                "org.apache.kafka.common.security.scram.ScramLoginModule"
                        + " required username=\"acme-x1-user\" password=\"p@ss\";",
                factory.capturedProperties.get("sasl.jaas.config"));
        assertNotNull(factory.capturedStaticHeaders);
        assertEquals(SERVER_ID, NxHeaders.decodeUuid(factory.capturedStaticHeaders.get(NxHeaders.NX_SERVER_ID)));
    }

    @Test
    void init_shouldForwardEmptyHeaders_whenServerIdAbsent() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        KafkaInitializer init = new KafkaInitializer(factory);

        init.init(kafkaCredentials("u", "p"), "client-id", Collections.emptyMap(), s -> {});

        assertNotNull(factory.capturedStaticHeaders);
        assertTrue(factory.capturedStaticHeaders.isEmpty());
    }

    @Test
    void init_shouldReturnPostBuildState_whenFactoryReportsDisconnected() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        factory.postBuildState = KafkaState.DISCONNECTED;
        KafkaInitializer init = new KafkaInitializer(factory);

        KafkaState result = init.init(
                kafkaCredentials("user", "pass"), "nx-gs-adapter-acme-acme-x1", Collections.emptyMap(), state -> {});

        assertEquals(KafkaState.DISCONNECTED, result);
    }

    @Test
    void buildJaas_shouldFormatScramLoginModuleString() {
        assertEquals(
                "org.apache.kafka.common.security.scram.ScramLoginModule required"
                        + " username=\"user\" password=\"pass\";",
                KafkaInitializer.buildJaas("user", "pass"));
    }
}
