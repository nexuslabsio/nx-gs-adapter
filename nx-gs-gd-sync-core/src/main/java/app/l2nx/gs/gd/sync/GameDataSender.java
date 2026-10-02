package app.l2nx.gs.gd.sync;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Pre-built record because gd-sync stamps the {@code Nx-Message-Type} header on every record. */
@FunctionalInterface
public interface GameDataSender {

    /** Must be non-blocking; the callback runs on the Kafka I/O thread. */
    void send(ProducerRecord<byte[], Object> record, Callback callback);
}
