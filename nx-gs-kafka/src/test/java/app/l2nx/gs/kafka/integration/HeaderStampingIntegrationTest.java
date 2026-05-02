package app.l2nx.gs.kafka.integration;

import app.l2nx.gs.kafka.producer.NxProducer;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link NxProducer#create(Map, Gson, Map)} stamps the configured
 * static headers on every outbound record across every {@code send(...)} overload
 * and {@link NxProducer#sendRecord(ProducerRecord)}.
 *
 * <p>Uses a raw {@code byte[]} value rather than the {@code NxHeaders} contract
 * from {@code nx-gs-adapter-api} so {@code nx-gs-kafka} stays a generic Kafka
 * facade with no adapter-api dependency on its test classpath.</p>
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class HeaderStampingIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            "confluentinc/cp-kafka:7.7.0"
    );

    private static final String HEADER_NAME = "X-Test-Static-Header";
    private static final byte[] HEADER_VALUE = new byte[]{
            0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef,
            (byte) 0xfe, (byte) 0xdc, (byte) 0xba, (byte) 0x98, 0x76, 0x54, 0x32, 0x10
    };

    private NxProducer producer;

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void send_topicMessage_shouldStampHeader() {
        String topic = "test.header-stamp.fire-forget";
        producer = createStampingProducer();

        producer.send(topic, new TestEvent("p1", 1));

        assertHeaderPresent(consumeOne(topic, "g1"));
    }

    @Test
    void send_topicKeyMessage_shouldStampHeader() {
        String topic = "test.header-stamp.string-key";
        producer = createStampingProducer();

        producer.send(topic, "k1", new TestEvent("p2", 2));

        assertHeaderPresent(consumeOne(topic, "g2"));
    }

    @Test
    void send_topicMessageCallback_shouldStampHeader() throws Exception {
        String topic = "test.header-stamp.callback";
        producer = createStampingProducer();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> err = new AtomicReference<>();

        producer.send(topic, new TestEvent("p3", 3), (m, e) -> {
            err.set(e);
            latch.countDown();
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(err.get());

        assertHeaderPresent(consumeOne(topic, "g3"));
    }

    @Test
    void send_topicKeyMessageCallback_shouldStampHeader() throws Exception {
        String topic = "test.header-stamp.string-key-callback";
        producer = createStampingProducer();
        CountDownLatch latch = new CountDownLatch(1);

        producer.send(topic, "k4", new TestEvent("p4", 4), (m, e) -> latch.countDown());
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertHeaderPresent(consumeOne(topic, "g4"));
    }

    @Test
    void send_topicBytesKeyMessageCallback_shouldStampHeader() throws Exception {
        String topic = "test.header-stamp.bytes-key";
        producer = createStampingProducer();
        CountDownLatch latch = new CountDownLatch(1);

        producer.send(topic, new byte[]{1, 2, 3, 4}, new TestEvent("p5", 5), (m, e) -> latch.countDown());
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertHeaderPresent(consumeOne(topic, "g5"));
    }

    @Test
    void sendRecord_shouldStampHeader_andPreserveExistingHeaders() {
        String topic = "test.header-stamp.send-record";
        producer = createStampingProducer();

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, "k6", new TestEvent("p6", 6));
        record.headers().add(new RecordHeader("Custom-Header", "v".getBytes()));

        producer.sendRecord(record);

        ConsumerRecord<String, byte[]> received = consumeOne(topic, "g6");
        assertHeaderPresent(received);
        Header custom = received.headers().lastHeader("Custom-Header");
        assertNotNull(custom);
        assertEquals("v", new String(custom.value()));
    }

    @Test
    void send_withoutStaticHeaders_shouldNotStampAnything() {
        String topic = "test.header-stamp.empty";
        Map<String, Object> props = producerProps("test-empty-headers");
        producer = NxProducer.create(props, new Gson(), Collections.emptyMap());

        producer.send(topic, new TestEvent("p7", 7));

        ConsumerRecord<String, byte[]> received = consumeOne(topic, "g7");
        assertNotNull(received);
        Header h = received.headers().lastHeader(HEADER_NAME);
        assertNull(h, "no test header expected");
    }

    private NxProducer createStampingProducer() {
        Map<String, Object> props = producerProps("test-stamping");
        Map<String, byte[]> headers = new HashMap<>();
        headers.put(HEADER_NAME, HEADER_VALUE);
        return NxProducer.create(props, new Gson(), headers);
    }

    private Map<String, Object> producerProps(String clientId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return props;
    }

    private void assertHeaderPresent(ConsumerRecord<String, byte[]> record) {
        assertNotNull(record, "expected a record");
        Header h = record.headers().lastHeader(HEADER_NAME);
        assertNotNull(h, HEADER_NAME + " header missing");
        assertArrayEquals(HEADER_VALUE, h.value());
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

    static class TestEvent {
        final String name;
        final int score;

        TestEvent(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    @SuppressWarnings("unused")
    private static byte[] copy(byte[] src) {
        return Arrays.copyOf(src, src.length);
    }
}
