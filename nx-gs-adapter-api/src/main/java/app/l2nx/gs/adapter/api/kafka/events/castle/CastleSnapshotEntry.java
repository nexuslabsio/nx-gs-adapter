package app.l2nx.gs.adapter.api.kafka.events.castle;

import app.l2nx.gs.adapter.api.kafka.events.schedule.RecurringSchedule;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One castle inside a {@link CastleSnapshotEvent}. {@code name} rides the wire because the platform has no castle
 * catalog. {@code ownerClanId} is {@code null} when unowned (host maps its no-owner sentinel). {@code nextSiegeAt},
 * {@code registrationEndsAt} (locks some hours before the siege) and {@code siegeEndsAt} are absolute and {@code null}
 * when the host does not expose them. {@code metadata} is an open map; consumers ignore unknown keys.
 */
public final class CastleSnapshotEntry {

    private final int castleId;
    private final @Nullable String name;
    private final @Nullable Long ownerClanId;
    private final @Nullable Instant nextSiegeAt;
    private final @Nullable Instant registrationEndsAt;
    private final @Nullable Instant siegeEndsAt;
    private final @Nullable Map<String, String> metadata;
    private final @Nullable RecurringSchedule schedule;

    public CastleSnapshotEntry(
            int castleId,
            @Nullable String name,
            @Nullable Long ownerClanId,
            @Nullable Instant nextSiegeAt,
            @Nullable Instant registrationEndsAt,
            @Nullable Instant siegeEndsAt,
            @Nullable Map<String, String> metadata,
            @Nullable RecurringSchedule schedule) {
        this.castleId = castleId;
        this.name = name;
        this.ownerClanId = ownerClanId;
        this.nextSiegeAt = nextSiegeAt;
        this.registrationEndsAt = registrationEndsAt;
        this.siegeEndsAt = siegeEndsAt;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
        this.schedule = schedule;
    }

    public int getCastleId() {
        return castleId;
    }

    public @Nullable String getName() {
        return name;
    }

    public @Nullable Long getOwnerClanId() {
        return ownerClanId;
    }

    public @Nullable Instant getNextSiegeAt() {
        return nextSiegeAt;
    }

    public @Nullable Instant getRegistrationEndsAt() {
        return registrationEndsAt;
    }

    public @Nullable Instant getSiegeEndsAt() {
        return siegeEndsAt;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    /** Derived from the castle's fixed weekly siege schedule; {@code null} when unavailable. */
    public @Nullable RecurringSchedule getSchedule() {
        return schedule;
    }

    public Builder toBuilder() {
        return new Builder()
                .castleId(castleId)
                .name(name)
                .ownerClanId(ownerClanId)
                .nextSiegeAt(nextSiegeAt)
                .registrationEndsAt(registrationEndsAt)
                .siegeEndsAt(siegeEndsAt)
                .metadata(metadata)
                .schedule(schedule);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CastleSnapshotEntry)) return false;
        CastleSnapshotEntry that = (CastleSnapshotEntry) o;
        return castleId == that.castleId
                && Objects.equals(name, that.name)
                && Objects.equals(ownerClanId, that.ownerClanId)
                && Objects.equals(nextSiegeAt, that.nextSiegeAt)
                && Objects.equals(registrationEndsAt, that.registrationEndsAt)
                && Objects.equals(siegeEndsAt, that.siegeEndsAt)
                && Objects.equals(metadata, that.metadata)
                && Objects.equals(schedule, that.schedule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                castleId, name, ownerClanId, nextSiegeAt, registrationEndsAt, siegeEndsAt, metadata, schedule);
    }

    @Override
    public String toString() {
        return "CastleSnapshotEntry[castleId=" + castleId
                + ", name=" + name
                + ", ownerClanId=" + ownerClanId
                + ", nextSiegeAt=" + nextSiegeAt
                + ", registrationEndsAt=" + registrationEndsAt
                + ", siegeEndsAt=" + siegeEndsAt
                + ", metadata=" + metadata
                + ", schedule=" + schedule + "]";
    }

    public static final class Builder {
        private int castleId;
        private @Nullable String name;
        private @Nullable Long ownerClanId;
        private @Nullable Instant nextSiegeAt;
        private @Nullable Instant registrationEndsAt;
        private @Nullable Instant siegeEndsAt;
        private @Nullable Map<String, String> metadata;
        private @Nullable RecurringSchedule schedule;

        public Builder castleId(int castleId) {
            this.castleId = castleId;
            return this;
        }

        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        public Builder ownerClanId(@Nullable Long ownerClanId) {
            this.ownerClanId = ownerClanId;
            return this;
        }

        public Builder nextSiegeAt(@Nullable Instant nextSiegeAt) {
            this.nextSiegeAt = nextSiegeAt;
            return this;
        }

        public Builder registrationEndsAt(@Nullable Instant registrationEndsAt) {
            this.registrationEndsAt = registrationEndsAt;
            return this;
        }

        public Builder siegeEndsAt(@Nullable Instant siegeEndsAt) {
            this.siegeEndsAt = siegeEndsAt;
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

        public CastleSnapshotEntry build() {
            return new CastleSnapshotEntry(
                    castleId, name, ownerClanId, nextSiegeAt, registrationEndsAt, siegeEndsAt, metadata, schedule);
        }
    }
}
