package app.l2nx.gs.kafka.integration;

import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.kafka.NxKafkaException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simulates Spring Kafka ReplyingKafkaTemplate protocol:
 * sends a request with kafka_replyTopic + kafka_correlationId headers,
 * verifies that nx-gs-kafka replies with the same correlationId.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class ReplyIntegrationTest {

    private static final String REQUEST_TOPIC = "test.request";
    private static final String REPLY_TOPIC = "test.replies";

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
    void reply_shouldSendToReplyTopic_withSameCorrelationId() throws Exception {
        NxKafka kafka = buildKafka("test-reply");

        // GS subscribes with reply support
        CountDownLatch handlerCalled = new CountDownLatch(1);
        kafka.subscribe(REQUEST_TOPIC, BalanceRequest.class, (request, replyTo) -> {
            replyTo.reply(new BalanceResponse(request.playerId, 5000));
            handlerCalled.countDown();
        });

        // Simulate Spring-side request with reply headers (binary correlationId)
        UUID correlationUuid = UUID.randomUUID();
        byte[] correlationBytes = uuidToBytes(correlationUuid);

        sendRequestWithReplyHeaders(
                REQUEST_TOPIC,
                "{\"playerId\":\"player42\"}",
                REPLY_TOPIC,
                correlationBytes
        );

        // Wait for handler to process
        assertTrue(handlerCalled.await(10, TimeUnit.SECONDS), "Handler was not called");

        // Consume reply from reply topic
        ConsumerRecord<String, byte[]> reply = consumeOne(REPLY_TOPIC, "test-reply-consumer");
        assertNotNull(reply, "Reply was not received");

        // Verify correlationId header is preserved
        Header correlationHeader = reply.headers().lastHeader("kafka_correlationId");
        assertNotNull(correlationHeader, "Reply missing kafka_correlationId header");
        assertArrayEquals(correlationBytes, correlationHeader.value());

        // Verify reply body
        String json = new String(reply.value(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"playerId\":\"player42\""));
        assertTrue(json.contains("\"balance\":5000"));
    }

    @Test
    void reply_shouldBeNoOp_whenNoReplyHeaders() throws Exception {
        String requestTopic = "test.request.noop";
        String replyTopic = "test.replies.noop";

        NxKafka kafka = buildKafka("test-reply-noop");

        CountDownLatch latch = new CountDownLatch(1);
        kafka.subscribe(requestTopic, BalanceRequest.class, (request, replyTo) -> {
            assertFalse(replyTo.hasReplyTopic());
            replyTo.reply(new BalanceResponse(request.playerId, 0)); // should be no-op
            latch.countDown();
        });

        // Send message WITHOUT reply headers
        sendPlainMessage(requestTopic, "{\"playerId\":\"player1\"}");

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Handler was not called");

        // Reply topic should have no messages
        ConsumerRecord<String, byte[]> reply = consumeOne(replyTopic, "test-noop-consumer");
        assertNull(reply, "Reply should not have been sent");
    }

    private NxKafka buildKafka(String clientId) {
        return NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId(clientId)
                .reconnect(false)
                .build();
    }

    private void sendRequestWithReplyHeaders(String topic, String json,
                                             String replyTopic, byte[] correlationId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer())) {
            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(topic, json.getBytes(StandardCharsets.UTF_8));
            record.headers().add("kafka_replyTopic", replyTopic.getBytes(StandardCharsets.UTF_8));
            record.headers().add("kafka_correlationId", correlationId);
            producer.send(record);
            producer.flush();
        }
    }

    private void sendPlainMessage(String topic, String json) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(topic, json.getBytes(StandardCharsets.UTF_8)));
            producer.flush();
        }
    }

    private ConsumerRecord<String, byte[]> consumeOne(String topic, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
            if (records.isEmpty()) {
                return null;
            }
            return records.iterator().next();
        }
    }

    private static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bytes;
    }

    static class BalanceRequest {
        String playerId;
    }

    static class BalanceResponse {
        final String playerId;
        final int balance;

        BalanceResponse(String playerId, int balance) {
            this.playerId = playerId;
            this.balance = balance;
        }
    }
}
