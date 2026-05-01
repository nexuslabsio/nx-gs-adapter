package app.l2nx.gs.runtime.sync.engine.publish;

import org.apache.kafka.clients.producer.Callback;

/**
 * Narrow Kafka publish abstraction so the engine compiles + tests without
 * touching {@code NxKafka}. Mirrors the shape of {@code db-sync}'s
 * {@code KafkaSender} so tests substitute a recording fake on either side.
 *
 * <p>Key is {@code byte[]} (8-byte big-endian {@code long}) — same wire shape
 * as {@code db-sync}. A {@code db.character} and {@code runtime.character}
 * row with the same source PK lands on the same partition of their respective
 * topics, simplifying platform-side joining.</p>
 */
@FunctionalInterface
public interface KafkaSender {

    void send(String topic, byte[] key, Object value, Callback callback);
}
