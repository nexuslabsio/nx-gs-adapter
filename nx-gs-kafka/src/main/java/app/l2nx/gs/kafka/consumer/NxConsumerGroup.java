package app.l2nx.gs.kafka.consumer;

import app.l2nx.gs.kafka.producer.NxProducer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Manages a single Kafka consumer with a dedicated daemon poll-loop thread
 * for one topic subscription. Each {@code subscribe()} call creates one instance.
 *
 * @param <T> the deserialized message type
 */
class NxConsumerGroup<T> implements NxConsumer {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final long POLL_ERROR_BACKOFF_MS = 1000;

    private final String topic;
    private final Class<T> type;
    private final BiConsumer<T, ReplyContext> handler;
    private final NxProducer producer;
    private final Gson gson;
    private final NxLog log;
    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread pollThread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    NxConsumerGroup(String topic, Class<T> type, BiConsumer<T, ReplyContext> handler,
                    NxProducer producer, Gson gson, Map<String, Object> consumerConfig) {
        this.topic = topic;
        this.type = type;
        this.handler = handler;
        this.producer = producer;
        this.gson = gson;
        this.log = NxLogFactory.getLogger(NxConsumerGroup.class);

        this.consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(Collections.singletonList(topic));

        this.pollThread = new Thread(this::pollLoop, "nx-gs-kafka-consumer-" + topic);
        pollThread.setDaemon(true);
        pollThread.start();

        log.debug("Consumer started for topic {}", topic);
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
            try {
                pollThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.debug("Consumer stopped for topic {}", topic);
        }
    }

    private void pollLoop() {
        try {
            while (running.get()) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        processRecord(record);
                    }
                } catch (WakeupException e) {
                    if (!running.get()) {
                        return;
                    }
                    log.warn("Unexpected wakeup on consumer for topic {}", topic);
                } catch (Exception e) {
                    if (!running.get()) {
                        return;
                    }
                    log.error("Consumer poll error for topic {}, retrying: {}", topic, e.getMessage());
                    try {
                        Thread.sleep(POLL_ERROR_BACKOFF_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Error closing consumer for topic {}: {}", topic, e.getMessage());
            }
        }
    }

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        try {
            byte[] value = record.value();
            if (value == null) {
                return;
            }
            T message = gson.fromJson(new String(value, StandardCharsets.UTF_8), type);
            ReplyContext replyContext = new ReplyContext(record, producer);
            handler.accept(message, replyContext);
        } catch (Exception e) {
            log.error("Error processing message from topic {} partition {} offset {}: {}",
                    topic, record.partition(), record.offset(), e.getMessage());
        }
    }
}
