package app.l2nx.gs.adapter.api.kafka.sync.db;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Typed CDC event for one row of one synced entity; {@code T} is the entity DTO class.
 *
 * Wire shape (Gson JSON): {@code {"entityName":"clan","pk":12345,"op":"UPDATED","payload":{...},"timestampEpochMs":1761661381123}}
 *
 * {@code entityName} is the singular domain name, not the SQL table. The Kafka key is {@code pk}
 * as 8-byte big-endian ({@code LongSerializer}). {@code op} is a string ({@code CREATED}/
 * {@code UPDATED}/{@code DELETED}) to avoid enum ordinal coupling; treat unknown values defensively.
 * {@code payload} is null for {@code DELETED}; topics use bounded retention, not compaction, so
 * consumers must handle {@code DELETED} explicitly. Equality delegates to {@code T.equals}.
 */
public final class SyncEvent<T> {

    private final String entityName;
    private final long pk;
    private final String op;
    private final @Nullable T payload;
    private final long timestampEpochMs;

    public SyncEvent(String entityName, long pk, String op, @Nullable T payload, long timestampEpochMs) {
        this.entityName = entityName;
        this.pk = pk;
        this.op = op;
        this.payload = payload;
        this.timestampEpochMs = timestampEpochMs;
    }

    public String getEntityName() {
        return entityName;
    }

    public long getPk() {
        return pk;
    }

    public String getOp() {
        return op;
    }

    public @Nullable T getPayload() {
        return payload;
    }

    public long getTimestampEpochMs() {
        return timestampEpochMs;
    }

    public Builder<T> toBuilder() {
        return new Builder<T>()
                .entityName(entityName)
                .pk(pk)
                .op(op)
                .payload(payload)
                .timestampEpochMs(timestampEpochMs);
    }

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyncEvent)) return false;
        SyncEvent<?> that = (SyncEvent<?>) o;
        return pk == that.pk
                && timestampEpochMs == that.timestampEpochMs
                && Objects.equals(entityName, that.entityName)
                && Objects.equals(op, that.op)
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityName, pk, op, payload, timestampEpochMs);
    }

    @Override
    public String toString() {
        return "SyncEvent[entityName=" + entityName
                + ", pk=" + pk
                + ", op=" + op
                + ", payload=" + payload
                + ", timestampEpochMs=" + timestampEpochMs + "]";
    }

    public static final class Builder<T> {
        private String entityName;
        private long pk;
        private String op;
        private @Nullable T payload;
        private long timestampEpochMs;

        public Builder<T> entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder<T> pk(long pk) {
            this.pk = pk;
            return this;
        }

        public Builder<T> op(String op) {
            this.op = op;
            return this;
        }

        public Builder<T> payload(@Nullable T payload) {
            this.payload = payload;
            return this;
        }

        public Builder<T> timestampEpochMs(long timestampEpochMs) {
            this.timestampEpochMs = timestampEpochMs;
            return this;
        }

        public SyncEvent<T> build() {
            return new SyncEvent<T>(entityName, pk, op, payload, timestampEpochMs);
        }
    }
}
