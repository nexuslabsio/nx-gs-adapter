package app.l2nx.gs.adapter.api.kafka.events.leveldata;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO riding the {@code character} family topic
 * ({@code <tenant>.gs.events.character}), multiplexed with
 * {@code CharacterPresenceEvent} / {@code CharacterDeathEvent} via the
 * {@code Nx-Message-Type} header, on a host-managed cadence (server startup +
 * datapack reload). The level table is a low-cadence once-per-start snapshot, so
 * it reuses the {@code character} topic rather than carrying its own. Carries a
 * point-in-time FULL snapshot of the server's level→required-exp progression
 * table — one {@link LevelExpEntry} per character level with the absolute
 * (cumulative) experience required to reach it.
 *
 * <p>The platform combines this per-server table with each character's raw exp
 * (carried by {@code CharacterRuntimeDto.exp}) to compute "% progress within the
 * current level":
 * {@code pct = (exp - requiredExp[level]) / (requiredExp[level + 1] - requiredExp[level])}.</p>
 *
 * <p><b>Full snapshot, not a delta.</b> Each event lists the complete current
 * level table. The platform consumer keeps last-known state per server and
 * replaces it on receipt; a level absent from a newer snapshot is dropped
 * (mark-and-sweep). The table changes rarely (only on rate / datapack edits),
 * so the cadence can be slow.</p>
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery) and order within-server by the embedded
 * timestamp.</p>
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
public final class LevelExpTableSnapshotEvent {

    private final UUID eventId;
    private final List<LevelExpEntry> levels;
    private final @Nullable Map<String, String> metadata;

    public LevelExpTableSnapshotEvent(
            UUID eventId, @Nullable List<LevelExpEntry> levels, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "LevelExpTableSnapshotEvent.eventId is required");
        this.levels = freezeList(levels);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the snapshot
     * occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Complete current level→required-exp table. Always non-null on read;
     * {@code null} passed to the constructor is normalized to an empty list.
     * The returned list is unmodifiable.
     */
    public List<LevelExpEntry> getLevels() {
        return levels;
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
        return new Builder().eventId(eventId).levels(levels).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<LevelExpEntry> freezeList(@Nullable List<LevelExpEntry> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<LevelExpEntry>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LevelExpTableSnapshotEvent)) return false;
        LevelExpTableSnapshotEvent that = (LevelExpTableSnapshotEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(levels, that.levels)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, levels, metadata);
    }

    @Override
    public String toString() {
        return "LevelExpTableSnapshotEvent[eventId=" + eventId + ", levels=" + levels + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private @Nullable List<LevelExpEntry> levels;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder levels(@Nullable List<LevelExpEntry> levels) {
            this.levels = levels;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public LevelExpTableSnapshotEvent build() {
            return new LevelExpTableSnapshotEvent(eventId, levels, metadata);
        }
    }
}
