package app.l2nx.gs.adapter.api.kafka.events.online;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO published to the {@code online} family topic
 * ({@code <tenant>.gs.events.serveronline}) on a host-driven cadence. Carries a
 * point-in-time breakdown of game-server population by activity bucket.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery) and order within-server by the embedded
 * timestamp.</p>
 *
 * <p>{@link #getBuckets() buckets} is an open map — keys SHOULD be drawn
 * from {@link WellKnownOnlineBuckets} where the host has the corresponding
 * concept; arbitrary additional keys are permitted for host-specific
 * buckets. There is no top-level {@code total} field: buckets can overlap
 * (a fishing player typically counts in {@code FISHING}, {@code REAL}, and
 * {@code ONLINE}), so the host publishes
 * {@link WellKnownOnlineBuckets#TOTAL} as an explicit map entry when it
 * tracks a meaningful total.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson can deserialize without {@code @JsonProperty}.</p>
 */
public final class OnlineSnapshotEvent extends OnlineEvent {

    private final UUID eventId;
    private final Map<String, Long> buckets;

    public OnlineSnapshotEvent(UUID eventId,
                               @Nullable Map<String, Long> buckets) {
        this.eventId = eventId;
        this.buckets = freezeMap(buckets);
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the
     * snapshot occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Bucket-key → count breakdown. Always non-null on read; {@code null}
     * passed to the constructor is normalized to an empty map. The returned
     * map is unmodifiable; mutation attempts throw
     * {@link UnsupportedOperationException}.
     *
     * <p>Keys: see {@link WellKnownOnlineBuckets} for the canonical set.
     * Values: non-negative long counts.</p>
     */
    public Map<String, Long> getBuckets() {
        return buckets;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .buckets(buckets);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, Long> freezeMap(@Nullable Map<String, Long> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, Long>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OnlineSnapshotEvent)) return false;
        OnlineSnapshotEvent that = (OnlineSnapshotEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, buckets);
    }

    @Override
    public String toString() {
        return "OnlineSnapshotEvent[eventId=" + eventId
                + ", buckets=" + buckets + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private @Nullable Map<String, Long> buckets;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder buckets(@Nullable Map<String, Long> buckets) {
            this.buckets = buckets;
            return this;
        }

        public OnlineSnapshotEvent build() {
            return new OnlineSnapshotEvent(eventId, buckets);
        }
    }
}
