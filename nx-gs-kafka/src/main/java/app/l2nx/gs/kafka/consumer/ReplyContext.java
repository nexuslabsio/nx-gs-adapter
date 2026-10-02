package app.l2nx.gs.kafka.consumer;

import app.l2nx.gs.kafka.producer.NxProducer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Reply support compatible with Spring Kafka {@code ReplyingKafkaTemplate}; reply() is a warn-logged no-op without a reply topic header.
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

    public boolean hasReplyTopic() {
        return replyTopic != null;
    }

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
            log.error("Failed to send reply to {}", replyTopic, e);
        }
    }

    private static String extractStringHeader(ConsumerRecord<?, ?> record, String key) {
        // Spring Kafka writes these headers raw, not JSON
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
