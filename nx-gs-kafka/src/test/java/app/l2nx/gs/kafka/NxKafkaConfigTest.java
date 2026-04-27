package app.l2nx.gs.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NxKafkaConfigTest {

    @AfterEach
    void tearDown() {
        try {
            NxKafka.instance().shutdown();
        } catch (NxKafkaException ignored) {
        }
    }

    @Test
    void build_shouldThrow_whenBrokersNotSet() {
        NxKafkaConfig.Builder builder = NxKafka.configure()
                .clientId("test");

        NxKafkaException ex = assertThrows(NxKafkaException.class, builder::build);
        assertTrue(ex.getMessage().contains("Brokers"));
    }

    @Test
    void build_shouldThrow_whenBrokersEmpty() {
        NxKafkaConfig.Builder builder = NxKafka.configure()
                .brokers("   ")
                .clientId("test");

        assertThrows(NxKafkaException.class, builder::build);
    }

    @Test
    void build_shouldUseDefaults_whenOnlyBrokersSet() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        assertNotNull(kafka);
        assertEquals(NxKafkaState.DISCONNECTED, kafka.state());
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
}
