package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO riding the {@code raid} family topic
 * ({@code <tenant>.gs.events.raid}) on a host-managed cadence, multiplexed with
 * {@code RaidKillEvent} via the {@code Nx-Message-Type} header. Carries a
 * point-in-time full snapshot of every tracked raid boss — open-world raid
 * bosses and grand / epic bosses — with each boss's current status and next
 * respawn time.
 *
 * <p><b>Full snapshot, not a delta.</b> Each event lists the complete current
 * set of tracked bosses. The platform consumer keeps last-known state per
 * server and replaces it on receipt; a boss absent from a newer snapshot is
 * dropped (mark-and-sweep). Because respawn times are absolute
 * ({@link BossRespawnEntry#getNextRespawnAt()} is an {@code Instant}), the
 * platform counts down locally and the cadence can be slow — bosses change
 * state rarely.</p>
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery) and order within-server by the embedded
 * timestamp.</p>
 *
 * <p>Cheat / custom cores that bypass the standard raid-boss spawn managers
 * won't appear here; this snapshot reflects the host's own respawn bookkeeping.</p>
 *
 * <p>{@link #getMetadata() metadata} is an optional open string→string map of
 * build-agnostic snapshot-level attributes. {@code null} when absent. Hosts MAY
 * publish arbitrary keys without an API release; consumers ignore keys they do
 * not understand.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
 */
public final class BossRespawnSnapshotEvent {

    private final UUID eventId;
    private final List<BossRespawnEntry> bosses;
    private final @Nullable Map<String, String> metadata;

    public BossRespawnSnapshotEvent(UUID eventId,
                                    @Nullable List<BossRespawnEntry> bosses,
                                    @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "BossRespawnSnapshotEvent.eventId is required");
        this.bosses = freezeList(bosses);
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
     * Complete current set of tracked bosses. Always non-null on read;
     * {@code null} passed to the constructor is normalized to an empty list.
     * The returned list is unmodifiable.
     */
    public List<BossRespawnEntry> getBosses() {
        return bosses;
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
                .bosses(bosses)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<BossRespawnEntry> freezeList(@Nullable List<BossRespawnEntry> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<BossRespawnEntry>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BossRespawnSnapshotEvent)) return false;
        BossRespawnSnapshotEvent that = (BossRespawnSnapshotEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(bosses, that.bosses)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, bosses, metadata);
    }

    @Override
    public String toString() {
        return "BossRespawnSnapshotEvent[eventId=" + eventId
                + ", bosses=" + bosses
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private @Nullable List<BossRespawnEntry> bosses;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder bosses(@Nullable List<BossRespawnEntry> bosses) {
            this.bosses = bosses;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public BossRespawnSnapshotEvent build() {
            return new BossRespawnSnapshotEvent(eventId, bosses, metadata);
        }
    }
}
