package app.l2nx.gs.adapter.api.kafka.events.gameevents;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Full snapshot of every configured recurring event, published to {@code <tenant>.gs.events.gameevents} on a
 * host-managed cadence.
 *
 * <p>Not a delta: the consumer replaces its per-server state on receipt, so an event absent from a newer snapshot is
 * dropped. Start times are absolute Instants, so the platform counts down locally and the cadence can be slow.</p>
 *
 * <p>{@code eventId} is a UUIDv7: its upper 48 bits carry the occurrence time (no {@code occurredAt} field), and consumers dedupe on it (at-least-once delivery).</p>
 *
 * <p>Java-8 POJO; {@code -parameters} preserves constructor parameter names so Jackson / Gson bind without
 * {@code @JsonProperty}.</p>
 */
public final class GameEventSnapshotEvent {

    private final UUID eventId;
    private final List<GameEventEntry> events;
    private final @Nullable Map<String, String> metadata;

    public GameEventSnapshotEvent(
            UUID eventId, @Nullable List<GameEventEntry> events, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "GameEventSnapshotEvent.eventId is required");
        this.events = freezeList(events);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    /**
     * Never {@code null}: a {@code null} constructor argument becomes an empty list. Unmodifiable.
     */
    public List<GameEventEntry> getEvents() {
        return events;
    }

    /**
     * Open snapshot-level attributes, unmodifiable, or {@code null} when absent.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).events(events).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<GameEventEntry> freezeList(@Nullable List<GameEventEntry> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<GameEventEntry>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GameEventSnapshotEvent)) return false;
        GameEventSnapshotEvent that = (GameEventSnapshotEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(events, that.events)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, events, metadata);
    }

    @Override
    public String toString() {
        return "GameEventSnapshotEvent[eventId=" + eventId + ", events=" + events + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private @Nullable List<GameEventEntry> events;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder events(@Nullable List<GameEventEntry> events) {
            this.events = events;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public GameEventSnapshotEvent build() {
            return new GameEventSnapshotEvent(eventId, events, metadata);
        }
    }
}
