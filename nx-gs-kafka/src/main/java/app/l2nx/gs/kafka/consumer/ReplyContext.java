package app.l2nx.gs.kafka.consumer;

import app.l2nx.gs.kafka.producer.NxProducer;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/**
 * Provides reply capability for request-reply messaging.
 * Compatible with Spring Kafka's {@code ReplyingKafkaTemplate} protocol.
 *
 * <p>Extracts {@code kafka_replyTopic} and {@code kafka_correlationId} headers
 * from the incoming record. Call {@link #reply(Object)} to send a JSON-serialized
 * response to the reply topic with the same correlation ID.</p>
 *
 * <p>If the incoming message has no reply headers, {@link #reply(Object)} is a
 * no-op with a warning log.</p>
 */
public class ReplyContext {

    static final String HEADER_REPLY_TOPIC = "kafka_replyTopic";
    static final String HEADER_CORRELATION_ID = "kafka_correlationId";

    private static final NxLog log = NxLogFactory.getLogger(ReplyContext.class);

    private final String replyTopic;
    private final byte[] correlationId;
    private final NxProducer producer;

    ReplyContext(ConsumerRecord<String, byte[]> record, NxProducer producer) {
        this.producer = producer;
        this.replyTopic = extractStringHeader(record, HEADER_REPLY_TOPIC);
        this.correlationId = extractRawHeader(record, HEADER_CORRELATION_ID);
    }

    /**
     * Returns {@code true} if the incoming message contains reply headers.
     */
    public boolean hasReplyTopic() {
        return replyTopic != null;
    }

    /**
     * Sends a reply to the requester's reply topic with the same correlation ID.
     * The response is serialized to JSON via Gson by the producer.
     *
     * <p>No-op with warning if the incoming message had no reply headers.</p>
     *
     * @param response the object to serialize and send as reply
     */
    public void reply(Object response) {
        if (replyTopic == null) {
            log.warn("Cannot reply: incoming message has no {} header", HEADER_REPLY_TOPIC);
            return;
        }

        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>(replyTopic, response);
            if (correlationId != null) {
                record.headers().add(HEADER_CORRELATION_ID, correlationId);
            }
            producer.sendRecord(record);
        } catch (Exception e) {
            log.error("Failed to send reply to {}: {}", replyTopic, e.getMessage());
        }
    }

    private static String extractStringHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static byte[] extractRawHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null) {
            return null;
        }
        return header.value();
    }
}
