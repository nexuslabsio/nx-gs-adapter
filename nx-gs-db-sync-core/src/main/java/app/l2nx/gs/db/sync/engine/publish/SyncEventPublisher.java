package app.l2nx.gs.db.sync.engine.publish;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Builds {@link SyncEvent} payloads for created / updated / deleted PKs and
 * forwards them to a {@link KafkaSender}, returning a per-PK
 * {@link CompletableFuture} so the engine can walk publish results at end of
 * cycle and only advance the snapshot for PKs whose publish actually
 * succeeded.
 *
 * <p>Wire-shape choices:</p>
 * <ul>
 *     <li>Kafka key = 8-byte big-endian encoding of the PK ({@code long}) —
 *     identical to {@code LongSerializer.serialize(...)} for any external
 *     producer that wants to write the same row. Per-PK partition assignment
 *     stays consistent across writers.</li>
 *     <li>{@code DELETED} events carry a non-null {@link SyncEvent} envelope
 *     with {@code payload=null}: the consumer still sees {@code entityName},
 *     {@code op="DELETED"}, and {@code timestampEpochMs} for audit, while the
 *     payload slot is explicitly null. Topics in this slice run with bounded
 *     retention (≤1 day) instead of log compaction, so the value-null
 *     tombstone optimization is intentionally not used.</li>
 * </ul>
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

    /**
     * 8-byte big-endian encoding — matches Kafka's {@code LongSerializer.serialize}
     * exactly. Allocates a fresh array per call (cheap, 8 bytes); pooling isn't
     * worthwhile because the buffer escapes into the Kafka producer's send queue.
     */
    static byte[] encodeKey(long pk) {
        return ByteBuffer.allocate(Long.BYTES).putLong(pk).array();
    }
}
