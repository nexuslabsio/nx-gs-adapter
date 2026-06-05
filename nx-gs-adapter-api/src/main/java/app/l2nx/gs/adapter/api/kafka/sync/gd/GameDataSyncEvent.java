package app.l2nx.gs.adapter.api.kafka.sync.gd;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * Self-contained wire envelope for the {@code gd} (game-data) sync stream — the
 * gd analogue of db-sync's {@link app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent}.
 * One envelope per Kafka record on {@code <tenant>.gd.sync.<entity>}; the
 * consumer dispatches on {@link #getOp()}.
 *
 * <p>Field names mirror {@code SyncEvent} so the two envelopes stay consistent:
 * {@code entityName}, {@code op} (string, not enum — decouples consumers from JVM
 * ordinals; producing module defines the constants), {@code pk}, {@code payload},
 * {@code timestampEpochMs}. gd adds {@code syncId} (snapshot id) and {@code count}
 * (on the terminal marker). Unlike {@code SyncEvent.pk} ({@code long}), gd's
 * {@code pk} is a nullable {@link Long} because the {@code SNAPSHOT_COMPLETE}
 * marker carries no row key.</p>
 *
 * <p><b>Identity is NOT in the body</b> — exactly like db/runtime sync: the owning
 * {@code serverId} rides the {@code Nx-Server-Id} Kafka header, and the owning
 * tenant is resolved from the topic-name slug.</p>
 *
 * <p>A snapshot is a stateless burst: the adapter generates a {@code syncId}
 * (UUIDv7), publishes one {@code UPSERT} record per entity ({@code pk} +
 * {@code payload}), then a single {@code SNAPSHOT_COMPLETE} record
 * ({@code count}). Records are keyed (Kafka partition key) by the server id so the
 * whole burst lands in one partition in order — the complete marker is processed
 * after every UPSERT. The consumer upserts each payload (stamping {@code syncId})
 * and, on the marker, deletes the server's rows whose stored {@code syncId}
 * differs.</p>
 *
 * @param <T> payload type ({@link app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate}
 *            for the {@code itemtemplate} entity)
 */
public final class GameDataSyncEvent<T> {

    private final String entityName;
    private final String op;
    private final UUID syncId;
    private final @Nullable Long pk;
    private final @Nullable T payload;
    private final @Nullable Integer count;
    private final long timestampEpochMs;

    public GameDataSyncEvent(String entityName,
                             String op,
                             UUID syncId,
                             @Nullable Long pk,
                             @Nullable T payload,
                             @Nullable Integer count,
                             long timestampEpochMs) {
        this.entityName = Objects.requireNonNull(entityName, "entityName");
        this.op = Objects.requireNonNull(op, "op");
        this.syncId = Objects.requireNonNull(syncId, "syncId");
        this.pk = pk;
        this.payload = payload;
        this.count = count;
        this.timestampEpochMs = timestampEpochMs;
    }

    /**
     * Entity name in singular form, e.g. {@code "itemtemplate"} (matches the db-sync entity-name style).
     */
    public String getEntityName() {
        return entityName;
    }

    /**
     * Operation: {@code "UPSERT"} or {@code "SNAPSHOT_COMPLETE"} (producing module owns the constants).
     */
    public String getOp() {
        return op;
    }

    /**
     * Monotonic snapshot id (UUIDv7); shared by every record of one snapshot.
     */
    public UUID getSyncId() {
        return syncId;
    }

    /**
     * Entity primary key on an {@code UPSERT}; {@code null} on the marker.
     */
    public @Nullable Long getPk() {
        return pk;
    }

    /**
     * Entity payload on an {@code UPSERT}; {@code null} on the marker.
     */
    public @Nullable T getPayload() {
        return payload;
    }

    /**
     * Item count on a {@code SNAPSHOT_COMPLETE} marker; {@code null} otherwise.
     */
    public @Nullable Integer getCount() {
        return count;
    }

    public long getTimestampEpochMs() {
        return timestampEpochMs;
    }

    public Builder<T> toBuilder() {
        return new Builder<T>()
                .entityName(entityName)
                .op(op)
                .syncId(syncId)
                .pk(pk)
                .payload(payload)
                .count(count)
                .timestampEpochMs(timestampEpochMs);
    }

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GameDataSyncEvent)) return false;
        GameDataSyncEvent<?> that = (GameDataSyncEvent<?>) o;
        return timestampEpochMs == that.timestampEpochMs
                && Objects.equals(entityName, that.entityName)
                && Objects.equals(op, that.op)
                && Objects.equals(syncId, that.syncId)
                && Objects.equals(pk, that.pk)
                && Objects.equals(payload, that.payload)
                && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityName, op, syncId, pk, payload, count, timestampEpochMs);
    }

    @Override
    public String toString() {
        return "GameDataSyncEvent[entityName=" + entityName + ", op=" + op + ", syncId=" + syncId
                + ", pk=" + pk + ", count=" + count + "]";
    }

    public static final class Builder<T> {
        private String entityName;
        private String op;
        private UUID syncId;
        private @Nullable Long pk;
        private @Nullable T payload;
        private @Nullable Integer count;
        private long timestampEpochMs;

        public Builder<T> entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder<T> op(String op) {
            this.op = op;
            return this;
        }

        public Builder<T> syncId(UUID syncId) {
            this.syncId = syncId;
            return this;
        }

        public Builder<T> pk(@Nullable Long pk) {
            this.pk = pk;
            return this;
        }

        public Builder<T> payload(@Nullable T payload) {
            this.payload = payload;
            return this;
        }

        public Builder<T> count(@Nullable Integer count) {
            this.count = count;
            return this;
        }

        public Builder<T> timestampEpochMs(long timestampEpochMs) {
            this.timestampEpochMs = timestampEpochMs;
            return this;
        }

        public GameDataSyncEvent<T> build() {
            return new GameDataSyncEvent<T>(entityName, op, syncId, pk, payload, count, timestampEpochMs);
        }
    }
}
