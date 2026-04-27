package app.l2nx.gs.kafka.integration;

import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.KafkaState;
import app.l2nx.gs.kafka.NxKafka;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class NxKafkaAdvancedIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            "confluentinc/cp-kafka:7.7.0"
    );

    @AfterEach
    void tearDown() {
        try {
            NxKafka.instance().shutdown();
        } catch (KafkaException ignored) {
        }
    }

    @Test
    void customGson_shouldAffectProducerSerialization() throws Exception {
        String topic = "test.custom-gson.producer";

        // Gson that serializes dates as "yyyy-MM-dd"
        Gson customGson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();

        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-gson-producer")
                .reconnect(false)
                .gson(customGson)
                .build();

        DateEvent event = new DateEvent("event1", new Date(1712534400000L)); // 2024-04-08
        kafka.send(topic, event);

        ConsumerRecord<String, byte[]> record = consumeOne(topic, "group-gson-prod");
        assertNotNull(record);
        String json = new String(record.value(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"date\":\"2024-04-08\""), "Expected date format yyyy-MM-dd, got: " + json);
    }

    @Test
    void customGson_shouldAffectConsumerDeserialization() throws Exception {
        String topic = "test.custom-gson.consumer";

        Gson customGson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();

        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-gson-consumer")
                .reconnect(false)
                .gson(customGson)
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DateEvent> received = new AtomicReference<>();

        kafka.subscribe(topic, DateEvent.class, event -> {
            received.set(event);
            latch.countDown();
        });

        publishJson(topic, "{\"name\":\"event1\",\"date\":\"2024-04-08\"}");

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Message was not received");
        assertNotNull(received.get().date, "Date should have been deserialized");
        assertEquals("event1", received.get().name);
    }

    @Test
    void onStateChange_shouldBeInvoked_onConnect() {
        List<KafkaState> states = new CopyOnWriteArrayList<>();

        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-listener-connect")
                .reconnect(false)
                .onStateChange(states::add)
                .build();

        assertTrue(kafka.isConnected());
        assertTrue(states.contains(KafkaState.CONNECTED));
    }

    @Test
    void onStateChange_shouldBeInvoked_onShutdown() {
        List<KafkaState> states = new CopyOnWriteArrayList<>();

        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-listener-shutdown")
                .reconnect(false)
                .onStateChange(states::add)
                .build();

        kafka.shutdown();

        assertTrue(states.contains(KafkaState.CLOSED));
    }

    @Test
    void onStateChange_shouldNotPropagate_listenerException() {
        NxKafka kafka = NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-listener-error")
                .reconnect(false)
                .onStateChange(state -> {
                    throw new RuntimeException("boom");
                })
                .build();

        // Should not throw despite listener exception
        assertDoesNotThrow(kafka::shutdown);
    }

    private void publishJson(String topic, String json) {
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

    static class DateEvent {
        String name;
        Date date;

        DateEvent() {
        }

        DateEvent(String name, Date date) {
            this.name = name;
            this.date = date;
        }
    }
}
