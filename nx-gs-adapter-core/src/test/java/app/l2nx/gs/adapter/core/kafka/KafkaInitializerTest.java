package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.rest.KafkaConfig;
import app.l2nx.gs.adapter.api.rest.Topics;
import app.l2nx.gs.kafka.KafkaState;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class KafkaInitializerTest {

    private static KafkaConfig kafkaConfig(String user, String password) {
        return KafkaConfig.builder()
                .bootstrap("kafka.l2nx.app:9092")
                .securityProtocol("SASL_SSL")
                .saslMechanism("SCRAM-SHA-256")
                .saslUsername(user)
                .saslPassword(password)
                .topics(new Topics(null))
                .build();
    }

    @Test
    void init_shouldComposePropertiesAndForwardListener() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        KafkaInitializer init = new KafkaInitializer(factory);
        Consumer<KafkaState> listener = state -> { /* test placeholder */ };

        KafkaState result = init.init(
                kafkaConfig("acme-x1-user", "p@ss"),
                "nx-gs-adapter-acme-acme-x1",
                listener);

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
    }

    @Test
    void init_shouldReturnPostBuildState_whenFactoryReportsDisconnected() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        factory.postBuildState = KafkaState.DISCONNECTED;
        KafkaInitializer init = new KafkaInitializer(factory);

        KafkaState result = init.init(kafkaConfig("user", "pass"),
                "nx-gs-adapter-acme-acme-x1", state -> {
                });

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
