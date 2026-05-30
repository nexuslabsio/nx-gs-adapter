package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO published to the {@code serveronline} family topic
 * ({@code <tenant>.gs.events.serveronline}) on a host-driven cadence. Carries
 * a point-in-time breakdown of game-server population by activity bucket.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery) and order within-server by the embedded
 * timestamp.</p>
 *
 * <p>{@link #getBuckets() buckets} is an open map. Every snapshot MUST
 * carry the required canonical keys
 * {@link WellKnownServerOnlineBuckets#TOTAL} and
 * {@link WellKnownServerOnlineBuckets#UNIQUE}; hosts SHOULD additionally
 * publish the optional canonical keys
 * ({@link WellKnownServerOnlineBuckets#OFFLINE_TRADE},
 * {@link WellKnownServerOnlineBuckets#FISHING}) when the corresponding
 * concept applies, and MAY publish arbitrary host-specific keys. There is
 * no top-level {@code total} field: buckets can overlap (e.g. a fishing
 * player typically also counts in {@code UNIQUE}), so consumers MUST NOT
 * derive any total as {@code sum(buckets)} — read the {@code TOTAL} entry
 * directly. See {@link WellKnownServerOnlineBuckets} for the soft
 * cross-bucket invariant.</p>
 *
 * <p>{@link #getMetadata() metadata} is a separate, optional open
 * string→string map of build-agnostic attributes describing this snapshot
 * (distinct from {@link #getBuckets() buckets}, which carries the numeric
 * population breakdown). {@code null} when absent. Hosts MAY publish arbitrary
 * keys without an API release; consumers ignore keys they do not understand.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson can deserialize without {@code @JsonProperty}.</p>
 */
public final class ServerOnlineSnapshotEvent {

    private final UUID eventId;
    private final Map<String, Long> buckets;
    private final @Nullable Map<String, String> metadata;

    public ServerOnlineSnapshotEvent(UUID eventId,
                                     @Nullable Map<String, Long> buckets,
                                     @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.buckets = freezeMap(buckets);
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
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
     * <p>Keys: see {@link WellKnownServerOnlineBuckets} for the canonical
     * set. Values: non-negative long counts.</p>
     */
    public Map<String, Long> getBuckets() {
        return buckets;
    }

    /**
     * Open string→string map of build-agnostic attributes about this
     * snapshot. {@code null} when absent. When non-null the returned map is
     * unmodifiable; mutation attempts throw
     * {@link UnsupportedOperationException}.
     *
     * <p>Distinct from {@link #getBuckets() buckets}: this carries no
     * population counts. Hosts MAY add arbitrary keys without an API release;
     * consumers ignore keys they do not understand.</p>
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .buckets(buckets)
                .metadata(metadata);
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
        if (!(o instanceof ServerOnlineSnapshotEvent)) return false;
        ServerOnlineSnapshotEvent that = (ServerOnlineSnapshotEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(buckets, that.buckets)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, buckets, metadata);
    }

    @Override
    public String toString() {
        return "ServerOnlineSnapshotEvent[eventId=" + eventId
                + ", buckets=" + buckets
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private @Nullable Map<String, Long> buckets;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder buckets(@Nullable Map<String, Long> buckets) {
            this.buckets = buckets;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public ServerOnlineSnapshotEvent build() {
            return new ServerOnlineSnapshotEvent(eventId, buckets, metadata);
        }
    }
}
