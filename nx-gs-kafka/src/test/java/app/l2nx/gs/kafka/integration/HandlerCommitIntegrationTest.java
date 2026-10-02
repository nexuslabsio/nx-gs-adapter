package app.l2nx.gs.kafka.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.NxKafka;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/** At-least-once: a throwing handler leaves the offset uncommitted and the record is redelivered. */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class HandlerCommitIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.0");

    @AfterEach
    void tearDown() {
        try {
            NxKafka.instance().shutdown();
        } catch (KafkaException ignored) {
        }
    }

    @Test
    void handlerThrowing_shouldRedeliverRecord_onRestart() throws Exception {
        String topic = "test.handler.no-commit";
        String groupId = "g-handler-no-commit";

        NxKafka first = buildKafka("first");
        AtomicInteger firstCount = new AtomicInteger();
        CountDownLatch firstSeen = new CountDownLatch(1);
        first.subscribe(topic, groupId, TestEvent.class, event -> {
            firstCount.incrementAndGet();
            firstSeen.countDown();
            throw new RuntimeException("boom");
        });

        publishJson(topic, "{\"name\":\"a\",\"score\":1}");
        assertTrue(firstSeen.await(15, TimeUnit.SECONDS), "First instance never received the record");
        Thread.sleep(1000);
        first.shutdown();

        NxKafka second = buildKafka("second");
        CountDownLatch secondSeen = new CountDownLatch(1);
        second.subscribe(topic, groupId, TestEvent.class, event -> secondSeen.countDown());

        assertTrue(secondSeen.await(15, TimeUnit.SECONDS), "Second instance should receive the uncommitted record");
    }

    @Test
    void midBatchFailure_shouldRedeliverFailedAndSubsequent_onRestart() throws Exception {
        // If record N fails mid-batch, later commits must not advance past it; without seek-on-failure restart skips N.
        String topic = "test.handler.midbatch";
        String groupId = "g-handler-midbatch";

        for (int i = 1; i <= 5; i++) {
            publishJson(topic, "{\"name\":\"r\",\"score\":" + i + "}");
        }

        NxKafka first = buildKafka("first-mb");
        List<Integer> firstSeen = new CopyOnWriteArrayList<Integer>();
        first.subscribe(topic, groupId, TestEvent.class, event -> {
            firstSeen.add(event.score);
            if (event.score == 3) {
                throw new RuntimeException("boom on 3");
            }
        });

        Thread.sleep(5000);
        first.shutdown();

        // Redelivery of already-acked records is allowed by at-least-once; losing score=3 is not.
        NxKafka second = buildKafka("second-mb");
        List<Integer> secondSeen = new CopyOnWriteArrayList<Integer>();
        CountDownLatch sawThree = new CountDownLatch(1);
        second.subscribe(topic, groupId, TestEvent.class, event -> {
            secondSeen.add(event.score);
            if (event.score == 3) {
                sawThree.countDown();
            }
        });

        assertTrue(
                sawThree.await(15, TimeUnit.SECONDS),
                "Second instance must redeliver the failed record (score=3); first saw " + firstSeen + ", second saw "
                        + secondSeen);
    }

    private NxKafka buildKafka(String clientId) {
        return NxKafka.configure()
                .brokers(KAFKA.getBootstrapServers())
                .clientId("test-handler-commit-" + clientId)
                .reconnect(false)
                .build();
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

    static class TestEvent {
        String name;
        int score;
    }
}
