package app.l2nx.gs.adapter.api.kafka.events.gameevents;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO published to the {@code gameevents} family topic
 * ({@code <tenant>.gs.events.gameevents}) on a host-managed cadence. Carries a
 * point-in-time full snapshot of every configured recurring event (TvT and
 * other mass-PvP / world events) with each event's schedule and run state.
 *
 * <p><b>Full snapshot, not a delta.</b> Each event lists the complete current
 * set of configured events. The platform consumer keeps last-known state per
 * server and replaces it on receipt; an event absent from a newer snapshot is
 * dropped (mark-and-sweep). Because start times are absolute
 * ({@link GameEventEntry#getNextStartAt()} is an {@code Instant}), the platform
 * counts down locally and the cadence can be slow.</p>
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery) and order within-server by the embedded
 * timestamp.</p>
 *
 * <p>The event vocabulary is build-agnostic — each {@link GameEventEntry}
 * carries a host-stable {@code code} plus an optional canonical
 * {@code event_kind} in its metadata (see {@link WellKnownGameEventMetadata}).</p>
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
public final class GameEventSnapshotEvent {

    private final UUID eventId;
    private final List<GameEventEntry> events;
    private final @Nullable Map<String, String> metadata;

    public GameEventSnapshotEvent(UUID eventId,
                                  @Nullable List<GameEventEntry> events,
                                  @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "GameEventSnapshotEvent.eventId is required");
        this.events = freezeList(events);
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
     * Complete current set of configured events. Always non-null on read;
     * {@code null} passed to the constructor is normalized to an empty list.
     * The returned list is unmodifiable.
     */
    public List<GameEventEntry> getEvents() {
        return events;
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
                .events(events)
                .metadata(metadata);
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
        return "GameEventSnapshotEvent[eventId=" + eventId
                + ", events=" + events
                + ", metadata=" + metadata + "]";
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
