package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Publishes one snapshot burst for a single gd entity: an {@link #OP_UPSERT} per template, then one
 * {@link #OP_SNAPSHOT_COMPLETE} marker with the count, all keyed by the raw 16-byte {@code serverId} so the
 * burst stays ordered on one partition. Never throws into the caller.
 */
public final class GameDataSnapshotPublisher {

    private static final NxLog log = NxLogFactory.getLogger(GameDataSnapshotPublisher.class);

    private static final String MESSAGE_TYPE = GameDataSyncEvent.class.getSimpleName();

    public static final String OP_UPSERT = "UPSERT";
    public static final String OP_SNAPSHOT_COMPLETE = "SNAPSHOT_COMPLETE";

    /** Null-provider grace before WARN escalates to ERROR; only hosts without a readiness provider reach the null path. */
    static final long NOT_READY_GRACE_MS = 15L * 60L * 1000L;

    private final GameDataSender sender;
    private final long notReadyGraceMs;
    private final LongSupplier clock;
    private final ConcurrentMap<String, EscalationTracker> nullTrackers =
            new ConcurrentHashMap<String, EscalationTracker>();

    public GameDataSnapshotPublisher(GameDataSender sender) {
        this(sender, NOT_READY_GRACE_MS, System::currentTimeMillis);
    }

    GameDataSnapshotPublisher(GameDataSender sender, long notReadyGraceMs, LongSupplier clock) {
        this.sender = sender;
        this.notReadyGraceMs = notReadyGraceMs;
        this.clock = clock;
    }

    /**
     * @param items null means "provider has nothing yet" (nothing published); empty is legal and emits count=0
     * @return {@code null} when nothing was published (items null / topic absent)
     */
    public <T> Result publishSnapshot(
            String entity, Collection<T> items, ToLongFunction<T> pkOf, UUID serverId, String topic) {
        if (topic == null || topic.isEmpty()) {
            log.warn("gd-sync snapshot for entity '{}' skipped — no topic configured", entity);
            return null;
        }
        if (items == null) {
            // no marker: count=0 would raise a platform alert
            reportNullSnapshot(entity);
            return null;
        }

        UUID syncId = UUIDv7.generate();
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
            // marker may be missing: report incomplete so the module shows DEGRADED
            log.error(
                    "gd-sync snapshot publish threw {} mid-burst (entity '{}', syncId {}) — partial burst sent",
                    t.getClass().getName(),
                    entity,
                    syncId,
                    t);
            return new Result(syncId, count, false);
        }

        // get(), not computeIfAbsent: don't allocate a tracker just to reset it
        EscalationTracker tracker = nullTrackers.get(entity);
        if (tracker != null) {
            tracker.reset();
        }
        log.info("gd-sync published snapshot for entity '{}': {} template(s), syncId {}", entity, count, syncId);
        return new Result(syncId, count, true);
    }

    private void reportNullSnapshot(String entity) {
        switch (trackerFor(entity).observe()) {
            case FIRST:
            case REPEAT:
                log.warn(
                        "gd-sync provider for entity '{}' has no snapshot yet — burst deferred, "
                                + "retrying on the next one",
                        entity);
                break;
            case ESCALATED:
                log.error(
                        "gd-sync provider for entity '{}' still returns null after {} minutes — "
                                + "no snapshot has ever been published for it",
                        entity,
                        notReadyGraceMs / 60000L);
                break;
            case SILENT:
                break;
        }
    }

    EscalationTracker trackerFor(String entity) {
        return nullTrackers.computeIfAbsent(entity, name -> new EscalationTracker(notReadyGraceMs, clock));
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

        /** {@code true} only when the SNAPSHOT_COMPLETE marker was handed to the sender. */
        public boolean complete() {
            return complete;
        }
    }
}
