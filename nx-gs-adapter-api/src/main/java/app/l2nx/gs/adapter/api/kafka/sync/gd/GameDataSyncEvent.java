package app.l2nx.gs.adapter.api.kafka.sync.gd;

import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Wire envelope for the {@code gd} sync stream, one per record on {@code <tenant>.gd.sync.<entity>}; mirrors
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent} except {@code pk} is nullable (the marker has no row key).
 * {@code serverId} rides the {@code Nx-Server-Id} header and the tenant comes from the topic slug, not the body.
 *
 * <p>A snapshot is a burst of {@code UPSERT}s then one {@code SNAPSHOT_COMPLETE}, keyed by server id so they
 * stay ordered in one partition; on the marker the consumer deletes rows whose stored {@code syncId} differs.</p>
 *
 * @param <T> entity payload type
 */
public final class GameDataSyncEvent<T> {

    private final String entityName;
    private final String op;
    private final UUID syncId;
    private final @Nullable Long pk;
    private final @Nullable T payload;
    private final @Nullable Integer count;
    private final long timestampEpochMs;

    public GameDataSyncEvent(
            String entityName,
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

    public String getEntityName() {
        return entityName;
    }

    /**
     * {@code UPSERT} or {@code SNAPSHOT_COMPLETE}; a string so consumers do not depend on JVM ordinals.
     */
    public String getOp() {
        return op;
    }

    /**
     * UUIDv7 shared by every record of one snapshot.
     */
    public UUID getSyncId() {
        return syncId;
    }

    /**
     * {@code null} on the marker.
     */
    public @Nullable Long getPk() {
        return pk;
    }

    /**
     * {@code null} on the marker.
     */
    public @Nullable T getPayload() {
        return payload;
    }

    /**
     * Set only on {@code SNAPSHOT_COMPLETE}.
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
        return "GameDataSyncEvent[entityName=" + entityName + ", op=" + op + ", syncId=" + syncId + ", pk=" + pk
                + ", count=" + count + "]";
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
