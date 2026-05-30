package app.l2nx.gs.adapter.api.kafka.events.castle;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO riding the {@code castle} family topic
 * ({@code <tenant>.gs.events.castle}) on a host-managed cadence, multiplexed with
 * {@link SiegeFinishedEvent} via the {@code Nx-Message-Type} header. Carries a
 * point-in-time full snapshot of every castle — its owning clan and next
 * scheduled siege.
 *
 * <p><b>Full snapshot, not a delta.</b> Each event lists the complete current set
 * of castles. The platform consumer keeps last-known state per server and
 * replaces it on receipt; a castle absent from a newer snapshot is dropped
 * (mark-and-sweep). Because {@link CastleSnapshotEntry#getNextSiegeAt()} is an
 * absolute {@code Instant}, the platform counts down locally and the cadence can
 * be slow — castle state changes rarely.</p>
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is encoded
 * in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe / order on the embedded
 * timestamp (at-least-once delivery).</p>
 *
 * <p>{@link #getMetadata() metadata} is an optional open string→string map of
 * build-agnostic snapshot-level attributes. {@code null} when absent.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
 */
public final class CastleSnapshotEvent {

    private final UUID eventId;
    private final List<CastleSnapshotEntry> castles;
    private final @Nullable Map<String, String> metadata;

    public CastleSnapshotEvent(UUID eventId,
                               @Nullable List<CastleSnapshotEntry> castles,
                               @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "CastleSnapshotEvent.eventId is required");
        this.castles = freezeList(castles);
        this.metadata = metadata == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the snapshot
     * occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Complete current set of castles. Always non-null on read; {@code null}
     * passed to the constructor is normalized to an empty list. The returned list
     * is unmodifiable.
     */
    public List<CastleSnapshotEntry> getCastles() {
        return castles;
    }

    /**
     * Optional open string→string map of build-agnostic attributes about this
     * snapshot. {@code null} when absent. When non-null the returned map is
     * unmodifiable.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .castles(castles)
                .metadata(metadata);
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
        return "CastleSnapshotEvent[eventId=" + eventId
                + ", castles=" + castles
                + ", metadata=" + metadata + "]";
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
