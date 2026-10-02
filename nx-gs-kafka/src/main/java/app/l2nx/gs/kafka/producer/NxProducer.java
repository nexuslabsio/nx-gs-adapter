package app.l2nx.gs.kafka.producer;

import com.google.gson.Gson;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Internal; use {@link app.l2nx.gs.kafka.NxKafka#send}. */
public interface NxProducer {

    void send(String topic, Object message);

    void send(String topic, String key, Object message);

    void send(String topic, Object message, Callback callback);

    void send(String topic, String key, Object message, Callback callback);

    /** Raw-bytes key; a null message sends a log-compaction tombstone. */
    void send(String topic, byte[] key, Object message, Callback callback);

    /** Used for reply records that need custom headers. */
    void sendRecord(ProducerRecord<String, Object> record);

    /** Static headers are appended to the record before send. */
    void sendBytesKeyRecord(ProducerRecord<byte[], Object> record, Callback callback);

    /** Blocks until buffered records reach the broker. */
    void flush();

    void close();

    static NxProducer create(Map<String, Object> config, Gson gson) {
        return new DefaultNxProducer(config, gson);
    }

    /** staticHeaders is copied defensively; may be empty, not null. */
    static NxProducer create(Map<String, Object> config, Gson gson, Map<String, byte[]> staticHeaders) {
        return new DefaultNxProducer(config, gson, staticHeaders);
    }

    /**
     * closeTimeout caps how long close() blocks on in-flight records, so shutdown cannot hang on an unreachable broker.
     */
    static NxProducer create(
            Map<String, Object> config, Gson gson, Map<String, byte[]> staticHeaders, Duration closeTimeout) {
        return new DefaultNxProducer(config, gson, staticHeaders, closeTimeout);
    }
}
