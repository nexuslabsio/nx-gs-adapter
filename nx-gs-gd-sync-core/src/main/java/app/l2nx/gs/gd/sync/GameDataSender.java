package app.l2nx.gs.gd.sync;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Narrow Kafka publish abstraction so the snapshot publisher compiles + tests
 * without touching {@code NxKafka}. The default impl bridges to
 * {@code NxKafka.instance().sendBytesKeyRecord(record, callback)}; tests
 * substitute a recording fake.
 *
 * <p>Takes a pre-built {@link ProducerRecord} (rather than a topic / key / value
 * triple) because gd-sync stamps the {@code Nx-Message-Type} header on every
 * record — same shape adapter-core's events publisher uses.</p>
 */
@FunctionalInterface
public interface GameDataSender {

    /**
     * Fire-and-forget send with a delivery callback. Implementations MUST be
     * non-blocking — Kafka client buffers internally. The callback is invoked
     * on the Kafka I/O thread.
     */
    void send(ProducerRecord<byte[], Object> record, Callback callback);
}
