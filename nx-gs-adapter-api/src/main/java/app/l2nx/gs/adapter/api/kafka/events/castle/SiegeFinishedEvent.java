package app.l2nx.gs.adapter.api.kafka.events.castle;

import java.time.Instant;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One event per ended siege on the {@code castle} family topic, multiplexed with {@link CastleSnapshotEvent}.
 * {@code eventId} MUST be a UUIDv7 (finish timestamp in the upper 48 bits; consumers dedupe on it). Partition key is the
 * {@code castleId} (8-byte big-endian).
 * {@code winnerClanId} is the clan holding the castle after the siege: captor on {@code captured}, prior owner on
 * {@code defended}, {@code null} on {@code draw}. The winner is always in its side's list, even if the engine
 * reclassified the captor out of the attacker list at siege end.
 */
public final class SiegeFinishedEvent {

    private final UUID eventId;
    private final int castleId;
    private final @Nullable String castleName;
    private final @Nullable Instant siegeStartedAt;
    private final String outcome;
    private final @Nullable Long winnerClanId;
    private final List<Long> attackerClanIds;
    private final List<Long> defenderClanIds;
    private final @Nullable Map<String, String> metadata;

    public SiegeFinishedEvent(
            UUID eventId,
            int castleId,
            @Nullable String castleName,
            @Nullable Instant siegeStartedAt,
            String outcome,
            @Nullable Long winnerClanId,
            @Nullable List<Long> attackerClanIds,
            @Nullable List<Long> defenderClanIds,
            @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "SiegeFinishedEvent.eventId is required");
        this.castleId = castleId;
        this.castleName = castleName;
        this.siegeStartedAt = siegeStartedAt;
        this.outcome = Objects.requireNonNull(outcome, "SiegeFinishedEvent.outcome is required");
        this.winnerClanId = winnerClanId;
        this.attackerClanIds = freezeList(attackerClanIds);
        this.defenderClanIds = freezeList(defenderClanIds);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public int getCastleId() {
        return castleId;
    }

    public @Nullable String getCastleName() {
        return castleName;
    }

    public @Nullable Instant getSiegeStartedAt() {
        return siegeStartedAt;
    }

    public String getOutcome() {
        return outcome;
    }

    public @Nullable Long getWinnerClanId() {
        return winnerClanId;
    }

    public List<Long> getAttackerClanIds() {
        return attackerClanIds;
    }

    public List<Long> getDefenderClanIds() {
        return defenderClanIds;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .castleId(castleId)
                .castleName(castleName)
                .siegeStartedAt(siegeStartedAt)
                .outcome(outcome)
                .winnerClanId(winnerClanId)
                .attackerClanIds(attackerClanIds)
                .defenderClanIds(defenderClanIds)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<Long> freezeList(@Nullable List<Long> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<Long>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SiegeFinishedEvent)) return false;
        SiegeFinishedEvent that = (SiegeFinishedEvent) o;
        return castleId == that.castleId
                && eventId.equals(that.eventId)
                && Objects.equals(castleName, that.castleName)
                && Objects.equals(siegeStartedAt, that.siegeStartedAt)
                && outcome.equals(that.outcome)
                && Objects.equals(winnerClanId, that.winnerClanId)
                && attackerClanIds.equals(that.attackerClanIds)
                && defenderClanIds.equals(that.defenderClanIds)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                eventId,
                castleId,
                castleName,
                siegeStartedAt,
                outcome,
                winnerClanId,
                attackerClanIds,
                defenderClanIds,
                metadata);
    }

    @Override
    public String toString() {
        return "SiegeFinishedEvent[eventId=" + eventId
                + ", castleId=" + castleId
                + ", castleName=" + castleName
                + ", siegeStartedAt=" + siegeStartedAt
                + ", outcome=" + outcome
                + ", winnerClanId=" + winnerClanId
                + ", attackerClanIds=" + attackerClanIds
                + ", defenderClanIds=" + defenderClanIds
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private int castleId;
        private @Nullable String castleName;
        private @Nullable Instant siegeStartedAt;
        private @Nullable String outcome;
        private @Nullable Long winnerClanId;
        private @Nullable List<Long> attackerClanIds;
        private @Nullable List<Long> defenderClanIds;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder castleId(int castleId) {
            this.castleId = castleId;
            return this;
        }

        public Builder castleName(@Nullable String castleName) {
            this.castleName = castleName;
            return this;
        }

        public Builder siegeStartedAt(@Nullable Instant siegeStartedAt) {
            this.siegeStartedAt = siegeStartedAt;
            return this;
        }

        public Builder outcome(String outcome) {
            this.outcome = outcome;
            return this;
        }

        public Builder winnerClanId(@Nullable Long winnerClanId) {
            this.winnerClanId = winnerClanId;
            return this;
        }

        public Builder attackerClanIds(@Nullable List<Long> attackerClanIds) {
            this.attackerClanIds = attackerClanIds;
            return this;
        }

        public Builder defenderClanIds(@Nullable List<Long> defenderClanIds) {
            this.defenderClanIds = defenderClanIds;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public SiegeFinishedEvent build() {
            return new SiegeFinishedEvent(
                    eventId,
                    castleId,
                    castleName,
                    siegeStartedAt,
                    outcome,
                    winnerClanId,
                    attackerClanIds,
                    defenderClanIds,
                    metadata);
        }
    }
}
