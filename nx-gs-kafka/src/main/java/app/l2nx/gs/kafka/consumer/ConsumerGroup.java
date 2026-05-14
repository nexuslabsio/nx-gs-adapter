package app.l2nx.gs.kafka.consumer;

import app.l2nx.gs.kafka.producer.NxProducer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
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
class ConsumerGroup<T> implements NxConsumer {

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
    private final boolean autoCommitEnabled;

    ConsumerGroup(String topic, Class<T> type, BiConsumer<T, ReplyContext> handler,
                  NxProducer producer, Gson gson, Map<String, Object> consumerConfig) {
        this.topic = topic;
        this.type = type;
        this.handler = handler;
        this.producer = producer;
        this.gson = gson;
        this.log = NxLogFactory.getLogger(ConsumerGroup.class);
        this.autoCommitEnabled = resolveAutoCommit(consumerConfig);
        if (this.autoCommitEnabled) {
            log.warn("auto.commit is enabled — at-least-once contract not enforced; failed handlers will silently drop records");
        }

        this.consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(Collections.singletonList(topic));

        this.pollThread = new Thread(this::pollLoop, "nx-gs-kafka-consumer-" + topic);
        pollThread.setDaemon(true);
        pollThread.setUncaughtExceptionHandler((t, ex) ->
                log.error("Uncaught exception on consumer thread {}", t.getName(), ex));
        pollThread.start();

        log.debug("Consumer started for topic {}", topic);
    }

    private static boolean resolveAutoCommit(Map<String, Object> consumerConfig) {
        Object raw = consumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (raw == null) {
            return false;
        }
        if (raw instanceof Boolean) {
            return (Boolean) raw;
        }
        return Boolean.parseBoolean(String.valueOf(raw));
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
                        if (!processRecord(record) && !autoCommitEnabled) {
                            // Handler failed — seek back to this offset so the next poll re-fetches
                            // it (and the rest of the batch). Without seek, Kafka's internal cursor
                            // advances past the failed record and subsequent successful commits in
                            // the same batch would skip it on restart.
                            consumer.seek(new TopicPartition(record.topic(), record.partition()),
                                    record.offset());
                            break;
                        }
                    }
                } catch (WakeupException e) {
                    if (!running.get()) {
                        return;
                    }
                    log.warn("Unexpected wakeup on consumer for topic {}", topic);
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (!running.get()) {
                        return;
                    }
                    log.error("Consumer poll error for topic {}, retrying", topic, t);
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
                log.warn("Error closing consumer for topic {}", topic, e);
            }
        }
    }

    private boolean processRecord(ConsumerRecord<String, byte[]> record) {
        T message;
        try {
            byte[] value = record.value();
            message = value == null ? null : gson.fromJson(new String(value, StandardCharsets.UTF_8), type);
        } catch (Throwable deserFailure) {
            // Permanent failure — payload won't parse on retry. Commit + skip.
            log.warn("Deserialization failed for topic {} partition {} offset {} — committing and skipping",
                    topic, record.partition(), record.offset(), deserFailure);
            if (!autoCommitEnabled) {
                commitOffset(record);
            }
            return true;
        }
        try {
            ReplyContext replyContext = new ReplyContext(record, producer);
            handler.accept(message, replyContext);
            if (!autoCommitEnabled) {
                commitOffset(record);
            }
            return true;
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return false;
            }
            log.warn("Handler failed for topic {} partition {} offset {} — record will be redelivered",
                    topic, record.partition(), record.offset(), t);
            return false;
        }
    }

    private void commitOffset(ConsumerRecord<String, byte[]> record) {
        try {
            consumer.commitSync(Collections.singletonMap(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)));
        } catch (Throwable t) {
            log.warn("Failed to commit offset for topic {} partition {} offset {}",
                    topic, record.partition(), record.offset(), t);
        }
    }
}
