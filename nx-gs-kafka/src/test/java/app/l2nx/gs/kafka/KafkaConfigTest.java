package app.l2nx.gs.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaConfigTest {

    @AfterEach
    void tearDown() {
        try {
            NxKafka.instance().shutdown();
        } catch (KafkaException ignored) {
        }
    }

    @Test
    void build_shouldThrow_whenBrokersNotSet() {
        KafkaConfig.Builder builder = NxKafka.configure().clientId("test");

        KafkaException ex = assertThrows(KafkaException.class, builder::build);
        assertTrue(ex.getMessage().contains("Brokers"));
    }

    @Test
    void build_shouldThrow_whenBrokersEmpty() {
        KafkaConfig.Builder builder = NxKafka.configure().brokers("   ").clientId("test");

        assertThrows(KafkaException.class, builder::build);
    }

    @Test
    void build_shouldUseDefaults_whenOnlyBrokersSet() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        assertNotNull(kafka);
        assertEquals(KafkaState.DISCONNECTED, kafka.state());
    }

    @Test
    void build_shouldAcceptCustomProperties() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .clientId("custom-id")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .property("security.protocol", "PLAINTEXT")
                .build();

        assertNotNull(kafka);
    }

    @Test
    void build_shouldConvertTimeUnits() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(2, TimeUnit.SECONDS)
                .reconnectInterval(1, TimeUnit.MINUTES)
                .reconnect(false)
                .build();

        assertNotNull(kafka);
    }

    @Test
    void build_shouldReject_whenConnectTimeoutExceedsBound() {
        KafkaConfig.Builder builder = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(2, TimeUnit.MINUTES)
                .reconnect(false);

        assertThrows(KafkaException.class, builder::build);
    }

    @Test
    void build_shouldReject_whenReconnectIntervalExceedsBound() {
        KafkaConfig.Builder builder = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnectInterval(10, TimeUnit.MINUTES)
                .reconnect(false);

        assertThrows(KafkaException.class, builder::build);
    }

    @Test
    void build_shouldAccept_customProducerCloseTimeout() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .producerCloseTimeout(Duration.ofSeconds(2))
                .build();

        assertNotNull(kafka);
    }
}
