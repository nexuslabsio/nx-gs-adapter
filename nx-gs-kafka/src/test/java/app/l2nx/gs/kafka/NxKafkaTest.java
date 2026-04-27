package app.l2nx.gs.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NxKafkaTest {

    @AfterEach
    void tearDown() {
        try {
            NxKafka.instance().shutdown();
        } catch (KafkaException ignored) {
        }
    }

    @Test
    void instance_shouldThrow_whenNotConfigured() {
        assertThrows(KafkaException.class, NxKafka::instance);
    }

    @Test
    void instance_shouldReturnSameObject() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        assertSame(kafka, NxKafka.instance());
    }

    @Test
    void build_shouldThrow_whenAlreadyConfigured() {
        NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        KafkaException ex = assertThrows(KafkaException.class, () ->
                NxKafka.configure()
                        .brokers("localhost:19999")
                        .connectTimeout(1, TimeUnit.SECONDS)
                        .reconnect(false)
                        .build()
        );
        assertTrue(ex.getMessage().contains("already configured"));
    }

    @Test
    void build_shouldSucceed_whenCalledAfterShutdown() {
        NxKafka first = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        first.shutdown();
        assertEquals(KafkaState.CLOSED, first.state());

        NxKafka second = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        assertNotSame(first, second);
        assertSame(second, NxKafka.instance());
    }

    @Test
    void state_shouldBeDisconnected_whenBrokerUnavailable() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        assertFalse(kafka.isConnected());
        assertEquals(KafkaState.DISCONNECTED, kafka.state());
    }

    @Test
    void shutdown_shouldBeIdempotent() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        kafka.shutdown();
        kafka.shutdown();

        assertEquals(KafkaState.CLOSED, kafka.state());
    }

    @Test
    void instance_shouldThrow_whenCalledAfterShutdown() {
        NxKafka kafka = NxKafka.configure()
                .brokers("localhost:19999")
                .connectTimeout(1, TimeUnit.SECONDS)
                .reconnect(false)
                .build();

        kafka.shutdown();

        assertThrows(KafkaException.class, NxKafka::instance);
    }
}
