package app.l2nx.gs.adapter.api.kafka.sync.db;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Typed CDC event published by the adapter for one row of one synced entity.
 * Generic parameter {@code T} is the entity's DTO class
 * (e.g. {@code SyncEvent<ClanDbDto>}); the platform-side consumer parameterizes
 * its {@code Consumer<SyncEvent<T>>} against the same {@code nx-gs-adapter-api}
 * artifact and gets compile-time payload guarantees.
 *
 * <p><b>Wire shape</b> (Gson-serialized JSON):</p>
 * <pre>
 *   {
 *     "entityName": "clan",
 *     "pk": 12345,
 *     "op": "UPDATED",
 *     "payload": { "clanId": 12345, "clanName": "Hellbound", ... },
 *     "timestampEpochMs": 1761661381123
 *   }
 * </pre>
 *
 * <p><b>Field semantics:</b></p>
 * <ul>
 *     <li>{@code entityName} — domain identifier in singular form
 *     ({@code "clan"}, {@code "character"}, {@code "item"}); matches
 *     {@code EntityMapping.entityName()} on the producer side. NOT the source
 *     SQL table name.</li>
 *     <li>{@code pk} — primary key as {@code long}. Engine reads via
 *     {@code rs.getLong(pkColumn)}. The Kafka message key is the same value
 *     encoded as 8 bytes big-endian (identical to
 *     {@code LongSerializer.serialize(topic, pk)}); consumers can decode with
 *     {@code LongDeserializer} for byte-equal partition + compaction-key parity
 *     across any future external writer.</li>
 *     <li>{@code op} — string enum on the wire: {@code "CREATED"},
 *     {@code "UPDATED"}, {@code "DELETED"}. String (not enum) keeps the platform
 *     consumer decoupled from JVM enum ordinals; consumers SHOULD treat unknown
 *     values defensively for forward-compat.</li>
 *     <li>{@code payload} — the row DTO. {@code null} for {@code DELETED}.
 *     Topics use bounded retention (typically ≤1 day), not log compaction, so
 *     consumers must explicitly handle the {@code DELETED} op rather than
 *     treating null-value tombstones as signal. Equality comparison (and
 *     hashing) of {@code SyncEvent} delegates to {@code T.equals} /
 *     {@code T.hashCode} — DTO authors MUST implement value semantics on their
 *     payload classes if downstream code compares {@code SyncEvent}s.</li>
 *     <li>{@code timestampEpochMs} — engine-side {@code System.currentTimeMillis()}
 *     at publish time. Epoch milliseconds keep the wire shape primitive
 *     (Gson-friendly with zero {@code TypeAdapter} setup) and consistent with
 *     {@code EntityStats.lastSyncEpochMs}.</li>
 * </ul>
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
