package app.l2nx.gs.db.sync.engine.publish;

import org.apache.kafka.clients.producer.Callback;

/**
 * Narrow Kafka publish abstraction so the engine compiles + tests without
 * touching {@code NxKafka}. The default impl bridges to
 * {@code NxKafka.instance().send(topic, byte[], value, callback)}; tests
 * substitute a recording fake.
 *
 * <p>Key is {@code byte[]} (not {@code String}) so the engine's primitive-PK
 * keying — 8-byte big-endian {@code long} — flows through unmodified to the
 * broker. Mixing this writer with another writer that uses {@code LongSerializer}
 * (or any other 8-byte big-endian encoder) for the same {@code long} value
 * lands them on the same partition.</p>
 */
@FunctionalInterface
public interface KafkaSender {

    /**
     * Fire-and-forget send with a delivery callback. Implementations MUST be
     * non-blocking — Kafka client buffers internally. The callback is invoked
     * on the Kafka I/O thread.
     */
    void send(String topic, byte[] key, Object value, Callback callback);
}
