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
     * Sends a byte-array-keyed message to the topic with a delivery callback.
     * Used by binary-key consumers (CDC tombstones, primitive-PK keying) where
     * the key is not a UTF-8 string. Same partition guarantee as the
     * String-keyed overload — partitioning is on the raw key bytes.
     *
     * @param topic    Kafka topic name
     * @param key      raw partition key bytes; may be null for round-robin
     * @param message  object to serialize as JSON; null for log-compaction tombstones
     * @param callback invoked on the Kafka I/O thread when the broker acknowledges or rejects the record
     */
    void send(String topic, byte[] key, Object message, Callback callback);

    /**
     * Sends a pre-built producer record (fire-and-forget). Used internally
     * for reply messages that need custom headers.
     *
     * @param record the producer record to send
     */
    void sendRecord(ProducerRecord<String, Object> record);

    /**
     * Sends a pre-built byte-array-keyed producer record with a delivery
     * callback. Used by callers that need full control over partition key,
     * per-record headers, and ack-tracking — e.g. {@code nx-gs-adapter-core}
     * stamping {@code Nx-Message-Type} per-event on per-character-keyed records.
     * Static headers are still appended to the record's headers before send.
     *
     * @param record   the producer record to send
     * @param callback invoked on the Kafka I/O thread when the broker acknowledges or rejects the record
     */
    void sendBytesKeyRecord(ProducerRecord<byte[], Object> record, Callback callback);

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

    /**
     * Creates a new producer that stamps the given static headers on every
     * outbound record. Use this overload to attach connection-scoped metadata
     * (e.g. {@code Nx-Server-Id} resolved once at adapter bootstrap) without
     * modifying every per-call site.
     *
     * <p>The {@code staticHeaders} map is defensively copied; mutations after
     * construction do not affect the producer.</p>
     *
     * @param config        Kafka producer configuration properties
     * @param gson          Gson instance for value serialization
     * @param staticHeaders headers added to every {@link ProducerRecord} before
     *                      send; may be empty but not {@code null}
     * @return a new producer instance
     */
    static NxProducer create(Map<String, Object> config, Gson gson,
                             Map<String, byte[]> staticHeaders) {
        return new DefaultNxProducer(config, gson, staticHeaders);
    }
}
