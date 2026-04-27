package app.l2nx.gs.kafka.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * Internal producer interface. Users should call
 * {@link app.l2nx.gs.kafka.NxKafka#send} methods instead.
 */
public interface NxProducer {

    /**
     * Sends a message to the topic (fire-and-forget). Errors are logged internally.
     *
     * @param topic   Kafka topic name
     * @param message object to serialize as JSON
     */
    void send(String topic, Object message);

    /**
     * Sends a keyed message to the topic (fire-and-forget).
     * Messages with the same key are guaranteed to land in the same partition.
     *
     * @param topic   Kafka topic name
     * @param key     partition key (e.g. player ID); may be null for round-robin
     * @param message object to serialize as JSON
     */
    void send(String topic, String key, Object message);

    /**
     * Sends a message to the topic with a delivery callback.
     *
     * @param topic    Kafka topic name
     * @param message  object to serialize as JSON
     * @param callback invoked on the Kafka I/O thread when the broker acknowledges or rejects the record
     */
    void send(String topic, Object message, Callback callback);

    /**
     * Sends a keyed message to the topic with a delivery callback.
     * Messages with the same key are guaranteed to land in the same partition.
     *
     * @param topic    Kafka topic name
     * @param key      partition key (e.g. player ID); may be null for round-robin
     * @param message  object to serialize as JSON
     * @param callback invoked on the Kafka I/O thread when the broker acknowledges or rejects the record
     */
    void send(String topic, String key, Object message, Callback callback);

    /**
     * Sends a pre-built producer record (fire-and-forget). Used internally
     * for reply messages that need custom headers.
     *
     * @param record the producer record to send
     */
    void sendRecord(ProducerRecord<String, Object> record);

    /**
     * Closes the underlying Kafka producer and releases resources.
     */
    void close();

    /**
     * Creates a new producer with the given Kafka client properties.
     *
     * @param config Kafka producer configuration properties
     * @return a new producer instance
     */
    static NxProducer create(Map<String, Object> config, Gson gson) {
        return new DefaultNxProducer(config, gson);
    }
}
