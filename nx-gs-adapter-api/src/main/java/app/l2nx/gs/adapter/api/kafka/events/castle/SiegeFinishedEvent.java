package app.l2nx.gs.adapter.api.kafka.events.castle;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.*;

/**
 * Wire DTO published to the {@code castle} family topic
 * ({@code <tenant>.gs.events.castle}) when a castle siege ends. One event per
 * siege, multiplexed with {@link CastleSnapshotEvent} via the
 * {@code Nx-Message-Type} header.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire finish-timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code finishedAt} field. Platform consumers dedupe on {@code eventId}
 * (at-least-once delivery). Partition key on the wire is the
 * {@link #getCastleId() castleId} (8-byte big-endian) — per-castle siege history
 * lands on one partition for ordered consumption.</p>
 *
 * <p>{@link #getOutcome() outcome} is an open string; canonical values in
 * {@link WellKnownSiegeOutcomes} ({@code captured} / {@code defended} /
 * {@code draw}). {@link #getWinnerClanId() winnerClanId} is the clan holding the
 * castle after the siege — the captor on {@code captured}, the prior owner on
 * {@code defended}, and {@code null} on {@code draw} (the castle was not won).
 * {@link #getAttackerClanIds() attackerClanIds} /
 * {@link #getDefenderClanIds() defenderClanIds} together are the full set of
 * clans that took part, registered on each side at siege end. When
 * {@code winnerClanId != null} the winner is guaranteed to appear in its side's
 * list (attacker on {@code captured}, defender on {@code defended}), so the
 * participant set always includes the winner even though the engine may have
 * reclassified the captor out of its attacker list at siege-end.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
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

    public SiegeFinishedEvent(UUID eventId,
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
        this.metadata = metadata == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the siege
     * finish timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Castle whose siege ended. Partition key on the wire (8-byte big-endian).
     */
    public int getCastleId() {
        return castleId;
    }

    /**
     * Display name at siege-end time; optional.
     */
    public @Nullable String getCastleName() {
        return castleName;
    }

    /**
     * Scheduled start of the siege that just ended, or {@code null} when unknown.
     */
    public @Nullable Instant getSiegeStartedAt() {
        return siegeStartedAt;
    }

    /**
     * Build-agnostic siege outcome — see {@link WellKnownSiegeOutcomes} for the
     * canonical {@code captured} / {@code defended} / {@code draw} values.
     */
    public String getOutcome() {
        return outcome;
    }

    /**
     * Clan holding the castle after the siege — captor on {@code captured},
     * defender on {@code defended}, {@code null} on {@code draw}.
     */
    public @Nullable Long getWinnerClanId() {
        return winnerClanId;
    }

    /**
     * Clans registered as attackers at siege end. Always non-null on read;
     * {@code null} passed to the constructor is normalized to an empty list. The
     * returned list is unmodifiable.
     */
    public List<Long> getAttackerClanIds() {
        return attackerClanIds;
    }

    /**
     * Clans registered as defenders at siege end (typically includes the owner).
     * Always non-null on read; {@code null} normalized to an empty list.
     */
    public List<Long> getDefenderClanIds() {
        return defenderClanIds;
    }

    /**
     * Optional open string→string attributes about this siege — {@code null}
     * when absent. Hosts MAY add arbitrary keys without an API release; consumers
     * ignore keys they do not understand.
     */
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
        return Objects.hash(eventId, castleId, castleName, siegeStartedAt, outcome,
                winnerClanId, attackerClanIds, defenderClanIds, metadata);
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
            return new SiegeFinishedEvent(eventId, castleId, castleName, siegeStartedAt,
                    outcome, winnerClanId, attackerClanIds, defenderClanIds, metadata);
        }
    }
}
