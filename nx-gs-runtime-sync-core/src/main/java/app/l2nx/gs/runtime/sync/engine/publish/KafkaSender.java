package app.l2nx.gs.runtime.sync.engine.publish;

import org.apache.kafka.clients.producer.Callback;

/** Key is the 8-byte big-endian pk, same as db-sync, so matching db/runtime rows share a partition index. */
@FunctionalInterface
public interface KafkaSender {

    void send(String topic, byte[] key, Object value, Callback callback);
}
