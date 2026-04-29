package app.l2nx.gs.db.sync.engine.publish;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

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
 *     producer that wants to write the same row. Log-compaction operates on
 *     raw key bytes so this guarantees per-PK partition + compaction-key
 *     consistency across writers.</li>
 *     <li>Tombstone — {@code DELETED} events carry {@code payload=null}; the
 *     compacted topic interprets a null value as "delete the entry for this
 *     key".</li>
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

    public <T> CompletableFuture<RecordMetadata> publish(EntityMapping<T> mapping,
                                                         String op,
                                                         long pk,
                                                         T dto,
                                                         String topic) {
        Object value;
        if (OP_DELETED.equals(op)) {
            value = null;
        } else {
            value = SyncEvent.<T>builder()
                    .entityName(mapping.entityName())
                    .pk(pk)
                    .op(op)
                    .payload(dto)
                    .timestampEpochMs(System.currentTimeMillis())
                    .build();
        }
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
