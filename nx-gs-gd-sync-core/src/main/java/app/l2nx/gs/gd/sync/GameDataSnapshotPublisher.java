package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;
import java.util.function.ToLongFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Builds one game-data snapshot burst for a single gd entity and forwards it to a
 * {@link GameDataSender}. Payload-agnostic: the caller supplies the entity name, the
 * template collection, and a primary-key extractor, so the same engine publishes
 * any gd entity (itemtemplate, npc, …).
 *
 * <p>A snapshot is a stateless burst keyed by {@code serverId} (raw 16-byte
 * UUID, so the whole burst lands in one partition, in order): one
 * {@link #OP_UPSERT} record per template, then a single
 * {@link #OP_SNAPSHOT_COMPLETE} marker carrying the count. Every record carries the
 * {@code Nx-Message-Type=GameDataSyncEvent} header for polymorphic dispatch on the
 * platform consumer.</p>
 *
 * <p>Never throws into the caller — a send failure is caught and logged.
 * {@code publishSnapshot} returns the number of UPSERTs handed to the sender so the
 * module can track per-entity stats.</p>
 */
public final class GameDataSnapshotPublisher {

    private static final NxLog log = NxLogFactory.getLogger(GameDataSnapshotPublisher.class);

    private static final String MESSAGE_TYPE = GameDataSyncEvent.class.getSimpleName();

    // gd op vocabulary — mirrors db-sync's SyncEventPublisher.OP_* string constants
    // (op rides the wire as a String, not a JVM enum).
    public static final String OP_UPSERT = "UPSERT";
    public static final String OP_SNAPSHOT_COMPLETE = "SNAPSHOT_COMPLETE";

    private final GameDataSender sender;

    public GameDataSnapshotPublisher(GameDataSender sender) {
        this.sender = sender;
    }

    /**
     * Publish the full burst for one entity's template set.
     *
     * @param entity   gd entity name (tags the wire envelope)
     * @param items    the provider's current template set (must be non-null; empty is legal)
     * @param pkOf     extracts the primary key from a template
     * @param serverId partition key (raw-16-byte encoded), keeps the burst on one partition
     * @param topic    destination topic
     * @return result carrying the generated {@code syncId} and template count, or
     * {@code null} when nothing was published (items null / topic absent).
     */
    public <T> Result publishSnapshot(
            String entity, Collection<T> items, ToLongFunction<T> pkOf, UUID serverId, String topic) {
        if (topic == null || topic.isEmpty()) {
            log.warn("gd-sync snapshot for entity '{}' skipped — no topic configured", entity);
            return null;
        }
        if (items == null) {
            // null breaks the never-null contract; aborting (no marker) avoids a count=0
            // SNAPSHOT_COMPLETE that would reconcile-delete the whole catalog. Empty is legal and
            // still emits count=0 via the loop below.
            log.error(
                    "gd-sync provider for entity '{}' returned null snapshot (contract violation) "
                            + "— burst aborted, no SNAPSHOT_COMPLETE emitted",
                    entity);
            return null;
        }

        UUID syncId = UUIDv7.generate();
        // Same raw-16-byte UUID encoding adapter-core stamps on the Nx-Server-Id
        // header — keeps the whole burst on one partition for a server.
        byte[] key = serverId != null ? NxHeaders.encodeUuid(serverId) : null;
        int count = 0;
        try {
            for (T item : items) {
                GameDataSyncEvent<T> upsert = GameDataSyncEvent.<T>builder()
                        .entityName(entity)
                        .op(OP_UPSERT)
                        .syncId(syncId)
                        .pk(pkOf.applyAsLong(item))
                        .payload(item)
                        .timestampEpochMs(System.currentTimeMillis())
                        .build();
                send(topic, key, upsert);
                count++;
            }

            GameDataSyncEvent<T> complete = GameDataSyncEvent.<T>builder()
                    .entityName(entity)
                    .op(OP_SNAPSHOT_COMPLETE)
                    .syncId(syncId)
                    .count(count)
                    .timestampEpochMs(System.currentTimeMillis())
                    .build();
            send(topic, key, complete);
        } catch (Throwable t) {
            // marker may not have been sent → report incomplete so the module shows DEGRADED, not a fresh sync
            log.error(
                    "gd-sync snapshot publish threw {} mid-burst (entity '{}', syncId {}) — partial burst sent",
                    t.getClass().getName(),
                    entity,
                    syncId,
                    t);
            return new Result(syncId, count, false);
        }

        log.info("gd-sync published snapshot for entity '{}': {} template(s), syncId {}", entity, count, syncId);
        return new Result(syncId, count, true);
    }

    private <T> void send(String topic, byte[] key, GameDataSyncEvent<T> value) {
        ProducerRecord<byte[], Object> record = new ProducerRecord<byte[], Object>(topic, key, value);
        record.headers().add(NxHeaders.NX_MESSAGE_TYPE, MESSAGE_TYPE.getBytes(StandardCharsets.UTF_8));
        sender.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.warn("gd-sync publish failed for topic {}: {}", topic, exception.getMessage(), exception);
            }
        });
    }

    /**
     * Outcome of one snapshot burst.
     */
    public static final class Result {
        private final UUID syncId;
        private final int count;
        private final boolean complete;

        public Result(UUID syncId, int count, boolean complete) {
            this.syncId = syncId;
            this.count = count;
            this.complete = complete;
        }

        public UUID syncId() {
            return syncId;
        }

        public int count() {
            return count;
        }

        /**
         * {@code true} when the full burst incl. the SNAPSHOT_COMPLETE marker was handed to the sender.
         */
        public boolean complete() {
            return complete;
        }
    }
}
