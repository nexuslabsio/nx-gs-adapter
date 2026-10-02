package app.l2nx.gs.db.sync.engine.publish;

import org.apache.kafka.clients.producer.Callback;

/**
 * Narrow publish abstraction so the engine tests without {@code NxKafka}. The key is {@code byte[]} (8-byte big-endian long),
 * so other writers using {@code LongSerializer} land the same PK on the same partition.
 */
@FunctionalInterface
public interface KafkaSender {

    /** Fire-and-forget; implementations must not block; the callback runs on the Kafka I/O thread. */
    void send(String topic, byte[] key, Object value, Callback callback);
}
