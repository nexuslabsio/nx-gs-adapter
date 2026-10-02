package app.l2nx.gs.adapter.api.kafka.events.gameevents;

import app.l2nx.gs.adapter.api.kafka.events.schedule.RecurringSchedule;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One configured recurring PvP / world event inside a {@link GameEventSnapshotEvent}, build-agnostic across event
 * engines. {@code code} is the stable per-event key the consumer upserts on; {@code metadata} keys are in
 * {@link WellKnownGameEventMetadata}.
 *
 * <p>Java-8 POJO; {@code -parameters} preserves constructor parameter names so Jackson / Gson bind without
 * {@code @JsonProperty}.</p>
 */
public final class GameEventEntry {

    private final String code;
    private final @Nullable String name;
    private final boolean enabled;
    private final @Nullable String status;
    private final @Nullable Instant nextStartAt;
    private final @Nullable Map<String, String> metadata;
    private final @Nullable RecurringSchedule schedule;

    public GameEventEntry(
            String code,
            @Nullable String name,
            boolean enabled,
            @Nullable String status,
            @Nullable Instant nextStartAt,
            @Nullable Map<String, String> metadata,
            @Nullable RecurringSchedule schedule) {
        this.code = code;
        this.name = name;
        this.enabled = enabled;
        this.status = status;
        this.nextStartAt = nextStartAt;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
        this.schedule = schedule;
    }

    public String getCode() {
        return code;
    }

    public @Nullable String getName() {
        return name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Open lifecycle phase ({@link WellKnownGameEventStatuses}), or {@code null} when the host engine exposes none.
     */
    public @Nullable String getStatus() {
        return status;
    }

    /**
     * Next scheduled start, or {@code null} when not scheduled (disabled or no upcoming occurrence).
     */
    public @Nullable Instant getNextStartAt() {
        return nextStartAt;
    }

    /**
     * Open string-to-string map, unmodifiable, or {@code null} when absent.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Recurring start rule ("every weekday(s) at HH:MM") derived from the event's
     * cron schedule, or {@code null} for one-off / seasonal patterns that don't
     * reduce to a weekly rule.
     */
    public @Nullable RecurringSchedule getSchedule() {
        return schedule;
    }

    public Builder toBuilder() {
        return new Builder()
                .code(code)
                .name(name)
                .enabled(enabled)
                .status(status)
                .nextStartAt(nextStartAt)
                .metadata(metadata)
                .schedule(schedule);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GameEventEntry)) return false;
        GameEventEntry that = (GameEventEntry) o;
        return enabled == that.enabled
                && Objects.equals(code, that.code)
                && Objects.equals(name, that.name)
                && Objects.equals(status, that.status)
                && Objects.equals(nextStartAt, that.nextStartAt)
                && Objects.equals(metadata, that.metadata)
                && Objects.equals(schedule, that.schedule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, name, enabled, status, nextStartAt, metadata, schedule);
    }

    @Override
    public String toString() {
        return "GameEventEntry[code=" + code
                + ", name=" + name
                + ", enabled=" + enabled
                + ", status=" + status
                + ", nextStartAt=" + nextStartAt
                + ", metadata=" + metadata
                + ", schedule=" + schedule + "]";
    }

    public static final class Builder {
        private String code;
        private @Nullable String name;
        private boolean enabled;
        private @Nullable String status;
        private @Nullable Instant nextStartAt;
        private @Nullable Map<String, String> metadata;
        private @Nullable RecurringSchedule schedule;

        public Builder code(String code) {
            this.code = code;
            return this;
        }

        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder status(@Nullable String status) {
            this.status = status;
            return this;
        }

        public Builder nextStartAt(@Nullable Instant nextStartAt) {
            this.nextStartAt = nextStartAt;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder schedule(@Nullable RecurringSchedule schedule) {
            this.schedule = schedule;
            return this;
        }

        public GameEventEntry build() {
            return new GameEventEntry(code, name, enabled, status, nextStartAt, metadata, schedule);
        }
    }
}
