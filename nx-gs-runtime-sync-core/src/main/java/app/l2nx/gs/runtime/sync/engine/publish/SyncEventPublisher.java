package app.l2nx.gs.runtime.sync.engine.publish;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.model.RuntimeEntityMapping;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;

/** Emits no DELETED: a logged-out character leaving the runtime snapshot is not a deletion (db-sync owns that). */
public final class SyncEventPublisher {

    public static final String OP_CREATED = "CREATED";
    public static final String OP_UPDATED = "UPDATED";

    private final KafkaSender sender;

    public SyncEventPublisher(KafkaSender sender) {
        this.sender = sender;
    }

    public <T> CompletableFuture<RecordMetadata> publish(
            RuntimeEntityMapping<T> mapping, String op, long pk, T dto, String topic) {
        SyncEvent<T> value = SyncEvent.<T>builder()
                .entityName(mapping.entityName())
                .pk(pk)
                .op(op)
                .payload(dto)
                .timestampEpochMs(System.currentTimeMillis())
                .build();
        CompletableFuture<RecordMetadata> future = new CompletableFuture<RecordMetadata>();
        try {
            // send() may block briefly when producer buffer.memory is saturated; flush-seconds only budgets the ack
            // wait
            sender.send(topic, encodeKey(pk), value, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(metadata);
                }
            });
        } catch (RuntimeException synchronousFailure) {
            future.completeExceptionally(synchronousFailure);
        }
        return future;
    }

    static byte[] encodeKey(long pk) {
        return ByteBuffer.allocate(Long.BYTES).putLong(pk).array();
    }
}
