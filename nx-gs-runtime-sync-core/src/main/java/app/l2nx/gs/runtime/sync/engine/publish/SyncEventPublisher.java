package app.l2nx.gs.runtime.sync.engine.publish;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Builds {@link SyncEvent} payloads for created / updated PKs and forwards them
 * to a {@link KafkaSender}, returning a per-PK {@link CompletableFuture} so the
 * engine can walk publish results at end of cycle and only advance the snapshot
 * for PKs whose publish actually succeeded.
 *
 * <p>Runtime-sync emits no DELETED events — db-sync owns "permanently gone"
 * semantics. A logged-out character disappearing from the runtime snapshot is
 * not a deletion.</p>
 */
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
            // sender.send may block briefly in producer.accumulator.append when
            // buffer.memory is saturated — acceptable on this daemon thread.
            // publish-flush-seconds is the post-send (await ack) budget; pre-send
            // backpressure is bounded by producer.buffer.memory at the Kafka side.
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
