package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate;
import app.l2nx.gs.adapter.api.spi.ItemTemplateProvider;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;

/**
 * Builds one game-data snapshot burst for a single {@link ItemTemplateProvider}
 * and forwards it to a {@link GameDataSender}.
 *
 * <p>A snapshot is a stateless burst keyed by {@code serverId} (raw 16-byte
 * UUID, so the whole burst lands in one partition, in order): one
 * {@link #OP_UPSERT} record per item template, then a single
 * {@link #OP_SNAPSHOT_COMPLETE} marker carrying the count. Every
 * record carries the {@code Nx-Message-Type=GameDataSyncEvent} header for
 * polymorphic dispatch on the platform consumer.</p>
 *
 * <p>Never throws into the caller — a provider / send failure is caught and
 * logged. {@code publishSnapshot} returns the number of UPSERTs handed to the
 * sender so the module can track {@code itemsPublished}.</p>
 */
public final class GameDataSnapshotPublisher {

    private static final NxLog log = NxLogFactory.getLogger(GameDataSnapshotPublisher.class);

    private static final String MESSAGE_TYPE = "GameDataSyncEvent";

    // gd op vocabulary — mirrors db-sync's SyncEventPublisher.OP_* string constants
    // (op rides the wire as a String, not a JVM enum).
    public static final String OP_UPSERT = "UPSERT";
    public static final String OP_SNAPSHOT_COMPLETE = "SNAPSHOT_COMPLETE";

    private final GameDataSender sender;

    public GameDataSnapshotPublisher(GameDataSender sender) {
        this.sender = sender;
    }

    /**
     * Pull the provider's current template set and publish the full burst.
     *
     * @return result carrying the generated {@code syncId} and item count, or
     * {@code null} when nothing was published (provider yielded null / the topic
     * was absent / a failure was caught).
     */
    public Result publishSnapshot(ItemTemplateProvider provider, UUID serverId, String topic) {
        if (provider == null) {
            log.warn("gd-sync snapshot skipped — provider is null");
            return null;
        }
        if (topic == null || topic.isEmpty()) {
            log.warn("gd-sync snapshot for entity '{}' skipped — no topic configured", provider.entityName());
            return null;
        }
        String entity;
        Collection<ItemTemplate> items;
        try {
            entity = provider.entityName();
            items = provider.snapshot();
        } catch (Throwable t) {
            log.error("ItemTemplateProvider threw {} pulling snapshot — gd-sync burst aborted",
                    t.getClass().getName(), t);
            return null;
        }
        if (items == null) {
            // null breaks the never-null contract; aborting (no marker) avoids a count=0
            // SNAPSHOT_COMPLETE that would reconcile-delete the whole catalog. Empty is legal and
            // still emits count=0 via the loop below.
            log.error("ItemTemplateProvider for entity '{}' returned null snapshot (contract violation) "
                    + "— gd-sync burst aborted, no SNAPSHOT_COMPLETE emitted", entity);
            return null;
        }

        UUID syncId = UUIDv7.generate();
        // Same raw-16-byte UUID encoding adapter-core stamps on the Nx-Server-Id
        // header — keeps the whole burst on one partition for a server.
        byte[] key = serverId != null ? NxHeaders.encodeUuid(serverId) : null;
        int count = 0;
        try {
            for (ItemTemplate item : items) {
                GameDataSyncEvent<ItemTemplate> upsert = GameDataSyncEvent.<ItemTemplate>builder()
                        .entityName(entity)
                        .op(OP_UPSERT)
                        .syncId(syncId)
                        .pk((long) item.getId())
                        .payload(item)
                        .timestampEpochMs(System.currentTimeMillis())
                        .build();
                send(topic, key, upsert);
                count++;
            }

            GameDataSyncEvent<ItemTemplate> complete = GameDataSyncEvent.<ItemTemplate>builder()
                    .entityName(entity)
                    .op(OP_SNAPSHOT_COMPLETE)
                    .syncId(syncId)
                    .count(count)
                    .timestampEpochMs(System.currentTimeMillis())
                    .build();
            send(topic, key, complete);
        } catch (Throwable t) {
            // marker may not have been sent → report incomplete so the module shows DEGRADED, not a fresh sync
            log.error("gd-sync snapshot publish threw {} mid-burst (entity '{}', syncId {}) — partial burst sent",
                    t.getClass().getName(), entity, syncId, t);
            return new Result(syncId, count, false);
        }

        log.info("gd-sync published snapshot for entity '{}': {} item(s), syncId {}", entity, count, syncId);
        return new Result(syncId, count, true);
    }

    private void send(String topic, byte[] key, GameDataSyncEvent<ItemTemplate> value) {
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
