package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import app.l2nx.gs.adapter.api.kafka.events.serveronline.model.WellKnownServerOnlineBuckets;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Periodic population breakdown by activity bucket, on the {@code serveronline} family topic.
 *
 * <p>{@code eventId} MUST be a UUIDv7 (upper 48 bits = timestamp); consumers dedupe on it
 * (at-least-once) and order within a server by it.</p>
 *
 * <p>{@code buckets} is an open map and MUST carry {@link WellKnownServerOnlineBuckets#TOTAL} and
 * {@link WellKnownServerOnlineBuckets#UNIQUE}. Buckets can overlap, so consumers MUST NOT derive a
 * total as {@code sum(buckets)}; read {@code TOTAL}.</p>
 *
 * <p>{@code metadata} is a separate open string map of snapshot attributes (no counts); hosts MAY add
 * keys without an API release, consumers ignore unknown ones.</p>
 */
public final class ServerOnlineSnapshotEvent {

    private final UUID eventId;
    private final Map<String, Long> buckets;
    private final @Nullable Map<String, String> metadata;

    public ServerOnlineSnapshotEvent(
            UUID eventId, @Nullable Map<String, Long> buckets, @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.buckets = freezeMap(buckets);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /** MUST be a UUIDv7. */
    public UUID getEventId() {
        return eventId;
    }

    /** Non-negative counts. Never null (a {@code null} constructor argument becomes an empty map); unmodifiable. */
    public Map<String, Long> getBuckets() {
        return buckets;
    }

    /** Unmodifiable when non-null. */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).buckets(buckets).metadata(metadata);
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
        return "ServerOnlineSnapshotEvent[eventId=" + eventId + ", buckets=" + buckets + ", metadata=" + metadata + "]";
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
