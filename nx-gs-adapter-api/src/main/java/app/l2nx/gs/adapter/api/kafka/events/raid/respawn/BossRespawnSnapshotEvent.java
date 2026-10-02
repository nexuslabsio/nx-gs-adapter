package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Periodic full snapshot of every tracked raid / epic boss, on the {@code raid} family topic
 * (multiplexed with {@code RaidKillEvent} via the {@code Nx-Message-Type} header).
 *
 * <p>Full snapshot, not a delta: the consumer replaces last-known state per server and drops bosses
 * absent from a newer snapshot. Respawn times are absolute, so the cadence can be slow.</p>
 *
 * <p>{@code eventId} MUST be a UUIDv7 (upper 48 bits = timestamp); consumers dedupe on it
 * (at-least-once) and order within a server by it. Bosses spawned outside the standard spawn managers
 * (cheat / custom cores) are not reported.</p>
 *
 * <p>{@code metadata} is an open string map; hosts MAY add keys without an API release, consumers
 * ignore unknown ones.</p>
 */
public final class BossRespawnSnapshotEvent {

    private final UUID eventId;
    private final List<BossRespawnEntry> bosses;
    private final @Nullable Map<String, String> metadata;

    public BossRespawnSnapshotEvent(
            UUID eventId, @Nullable List<BossRespawnEntry> bosses, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "BossRespawnSnapshotEvent.eventId is required");
        this.bosses = freezeList(bosses);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /** MUST be a UUIDv7. */
    public UUID getEventId() {
        return eventId;
    }

    /** Never null; a {@code null} constructor argument becomes an empty list. Unmodifiable. */
    public List<BossRespawnEntry> getBosses() {
        return bosses;
    }

    /** Unmodifiable when non-null. */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).bosses(bosses).metadata(metadata);
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
        return "BossRespawnSnapshotEvent[eventId=" + eventId + ", bosses=" + bosses + ", metadata=" + metadata + "]";
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
