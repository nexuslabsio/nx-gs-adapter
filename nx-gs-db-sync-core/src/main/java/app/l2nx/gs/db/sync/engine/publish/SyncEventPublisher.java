package app.l2nx.gs.db.sync.engine.publish;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.model.EntityMapping;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Builds {@link SyncEvent}s and forwards them to a {@link KafkaSender}, returning a per-PK future so the snapshot advances only for acked PKs.
 * Key is the 8-byte big-endian PK (equals {@code LongSerializer}); DELETED carries an envelope with {@code payload=null}, not a tombstone,
 * because topics use bounded retention (at most 1 day) instead of compaction.
 */
public final class SyncEventPublisher {

    public static final String OP_CREATED = "CREATED";
    public static final String OP_UPDATED = "UPDATED";
    public static final String OP_DELETED = "DELETED";

    private final KafkaSender sender;

    public SyncEventPublisher(KafkaSender sender) {
        this.sender = sender;
    }

    public <T> CompletableFuture<RecordMetadata> publish(
            EntityMapping<T> mapping, String op, long pk, T dto, String topic) {
        SyncEvent<T> value = SyncEvent.<T>builder()
                .entityName(mapping.entityName())
                .pk(pk)
                .op(op)
                .payload(dto)
                .timestampEpochMs(System.currentTimeMillis())
                .build();
        CompletableFuture<RecordMetadata> future = new CompletableFuture<RecordMetadata>();
        try {
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

    /** Matches {@code LongSerializer.serialize}; not pooled because the buffer escapes into the producer queue. */
    static byte[] encodeKey(long pk) {
        return ByteBuffer.allocate(Long.BYTES).putLong(pk).array();
    }
}
