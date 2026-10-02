package app.l2nx.gs.adapter.api.kafka.events.leveldata;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Full level to required-exp table, multiplexed on the {@code <tenant>.gs.events.character} topic via the
 * {@code Nx-Message-Type} header; published at server startup and datapack reload.
 *
 * <p>Not a delta: the consumer replaces its per-server table on receipt, so a level absent from a newer snapshot is
 * dropped. The table changes rarely, so the cadence can be slow.</p>
 *
 * <p>{@code eventId} is a UUIDv7: its upper 48 bits carry the occurrence time (no {@code occurredAt} field), and consumers dedupe on it (at-least-once delivery).</p>
 *
 * <p>Java-8 POJO; {@code -parameters} preserves constructor parameter names so Jackson / Gson bind without
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

    public UUID getEventId() {
        return eventId;
    }

    /**
     * Never {@code null}: a {@code null} constructor argument becomes an empty list. Unmodifiable.
     */
    public List<LevelExpEntry> getLevels() {
        return levels;
    }

    /**
     * Open snapshot-level attributes, unmodifiable, or {@code null} when absent.
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
