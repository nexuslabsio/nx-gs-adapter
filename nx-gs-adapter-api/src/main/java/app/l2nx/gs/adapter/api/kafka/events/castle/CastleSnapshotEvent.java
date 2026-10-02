package app.l2nx.gs.adapter.api.kafka.events.castle;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Full point-in-time snapshot of every castle on the {@code castle} family topic, multiplexed with
 * {@link SiegeFinishedEvent} via {@code Nx-Message-Type}. Not a delta: the consumer replaces its state and drops castles
 * absent from a newer snapshot. Absolute {@code nextSiegeAt} lets the platform count down locally, so cadence can be slow.
 * {@code eventId} MUST be a UUIDv7 (timestamp in the upper 48 bits, consumers dedupe/order on it).
 */
public final class CastleSnapshotEvent {

    private final UUID eventId;
    private final List<CastleSnapshotEntry> castles;
    private final @Nullable Map<String, String> metadata;

    public CastleSnapshotEvent(
            UUID eventId, @Nullable List<CastleSnapshotEntry> castles, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "CastleSnapshotEvent.eventId is required");
        this.castles = freezeList(castles);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public List<CastleSnapshotEntry> getCastles() {
        return castles;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).castles(castles).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<CastleSnapshotEntry> freezeList(@Nullable List<CastleSnapshotEntry> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<CastleSnapshotEntry>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CastleSnapshotEvent)) return false;
        CastleSnapshotEvent that = (CastleSnapshotEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(castles, that.castles)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, castles, metadata);
    }

    @Override
    public String toString() {
        return "CastleSnapshotEvent[eventId=" + eventId + ", castles=" + castles + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private @Nullable List<CastleSnapshotEntry> castles;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder castles(@Nullable List<CastleSnapshotEntry> castles) {
            this.castles = castles;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public CastleSnapshotEvent build() {
            return new CastleSnapshotEvent(eventId, castles, metadata);
        }
    }
}
