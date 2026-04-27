package app.l2nx.gs.kafka.integration;

import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.kafka.NxKafkaException;
import app.l2nx.gs.kafka.NxKafkaState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class NxKafkaIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            "confluentinc/cp-kafka:7.7.0"
    );

    @AfterEach
    void tearDown() {
        try {
            NxKafka.instance().shutdown();
        } catch (NxKafkaException ignored) {
        }
    }

    @Test
    void build_shouldConnect_whenBrokerRunning() {
        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-connect")
                .reconnect(false)
                .build();

        assertTrue(kafka.isConnected());
        assertEquals(NxKafkaState.CONNECTED, kafka.state());
    }

    @Test
    void build_shouldSetDisconnected_whenBrokerUnavailable() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .clientId("test-unavailable")
                .connectTimeout(2, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        assertFalse(kafka.isConnected());
        assertEquals(NxKafkaState.DISCONNECTED, kafka.state());
    }

    @Test
    void shutdown_shouldCloseCleanly_whenConnected() {
        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-shutdown")
                .reconnect(false)
                .build();

        assertTrue(kafka.isConnected());

        kafka.shutdown();

        assertEquals(NxKafkaState.CLOSED, kafka.state());
        assertThrows(NxKafkaException.class, NxKafka::instance);
    }

    @Test
    void healthCheck_shouldDetectDisconnectAndReconnect() throws InterruptedException {
        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-health")
                .reconnect(true)
                .reconnectInterval(2, TimeUnit.SECONDS)
                .build();

        assertTrue(kafka.isConnected());

        // Pause the container to simulate broker going down
        DockerClientFactory.instance().client()
                .pauseContainerCmd(KAFKA.getContainerId()).exec();

        try {
            awaitState(NxKafkaState.DISCONNECTED, 15000);
            assertFalse(kafka.isConnected());
        } finally {
            DockerClientFactory.instance().client()
                    .unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        // Wait for reconnection
        awaitState(NxKafkaState.CONNECTED, 15000);
        assertTrue(kafka.isConnected());
    }

    private void awaitState(NxKafkaState expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (NxKafka.instance().state() == expected) {
                return;
            }
            Thread.sleep(500);
        }
        assertEquals(expected, NxKafka.instance().state(),
                "Timed out waiting for state " + expected);
    }
}
