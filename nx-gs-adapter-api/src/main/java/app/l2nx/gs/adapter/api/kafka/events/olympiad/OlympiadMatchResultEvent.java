package app.l2nx.gs.adapter.api.kafka.events.olympiad;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Closed Olympiad 1v1 match — one event per participant (self-perspective).
 * Both per-participant events share {@link #getMatchId() matchId}.
 *
 * <p>Emitted for every points-changing conclusion including no-fight edge
 * cases (default / disconnect / timeout / both-offline) — see
 * {@link OlympiadMatchReason}. {@link #getFightStartedAt() fightStartedAt}
 * is {@code null} when no combat occurred.</p>
 */
public final class OlympiadMatchResultEvent {

    private final UUID eventId;
    private final UUID matchId;
    private final int olympiadCycle;
    private final OlympiadGameType gameType;

    private final long charId;
    private final int classId;
    private final @Nullable Long clanId;

    private final long opponentCharId;
    private final int opponentClassId;
    private final @Nullable Long opponentClanId;

    private final OlympiadMatchResult result;
    private final OlympiadMatchReason reason;

    private final int pointsBefore;
    private final int pointsAfter;

    private final int damageDealt;
    private final int opponentDamageDealt;
    private final @Nullable Instant fightStartedAt;
    private final long fightDurationSec;

    public OlympiadMatchResultEvent(UUID eventId,
                                    UUID matchId,
                                    int olympiadCycle,
                                    OlympiadGameType gameType,
                                    long charId,
                                    int classId,
                                    @Nullable Long clanId,
                                    long opponentCharId,
                                    int opponentClassId,
                                    @Nullable Long opponentClanId,
                                    OlympiadMatchResult result,
                                    OlympiadMatchReason reason,
                                    int pointsBefore,
                                    int pointsAfter,
                                    int damageDealt,
                                    int opponentDamageDealt,
                                    @Nullable Instant fightStartedAt,
                                    long fightDurationSec) {
        this.eventId = eventId;
        this.matchId = matchId;
        this.olympiadCycle = olympiadCycle;
        this.gameType = gameType;
        this.charId = charId;
        this.classId = classId;
        this.clanId = clanId;
        this.opponentCharId = opponentCharId;
        this.opponentClassId = opponentClassId;
        this.opponentClanId = opponentClanId;
        this.result = result;
        this.reason = reason;
        this.pointsBefore = pointsBefore;
        this.pointsAfter = pointsAfter;
        this.damageDealt = damageDealt;
        this.opponentDamageDealt = opponentDamageDealt;
        this.fightStartedAt = fightStartedAt;
        this.fightDurationSec = fightDurationSec;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getMatchId() {
        return matchId;
    }

    public int getOlympiadCycle() {
        return olympiadCycle;
    }

    public OlympiadGameType getGameType() {
        return gameType;
    }

    /**
     * Self-perspective character — partition key (8-byte BE).
     */
    public long getCharId() {
        return charId;
    }

    public int getClassId() {
        return classId;
    }

    /**
     * Snapshot at match time (clan affiliation can change between matches).
     */
    public @Nullable Long getClanId() {
        return clanId;
    }

    public long getOpponentCharId() {
        return opponentCharId;
    }

    public int getOpponentClassId() {
        return opponentClassId;
    }

    public @Nullable Long getOpponentClanId() {
        return opponentClanId;
    }

    public OlympiadMatchResult getResult() {
        return result;
    }

    public OlympiadMatchReason getReason() {
        return reason;
    }

    public int getPointsBefore() {
        return pointsBefore;
    }

    public int getPointsAfter() {
        return pointsAfter;
    }

    public int getDamageDealt() {
        return damageDealt;
    }

    public int getOpponentDamageDealt() {
        return opponentDamageDealt;
    }

    /**
     * {@code null} when no actual fight occurred.
     */
    public @Nullable Instant getFightStartedAt() {
        return fightStartedAt;
    }

    public long getFightDurationSec() {
        return fightDurationSec;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .matchId(matchId)
                .olympiadCycle(olympiadCycle)
                .gameType(gameType)
                .charId(charId)
                .classId(classId)
                .clanId(clanId)
                .opponentCharId(opponentCharId)
                .opponentClassId(opponentClassId)
                .opponentClanId(opponentClanId)
                .result(result)
                .reason(reason)
                .pointsBefore(pointsBefore)
                .pointsAfter(pointsAfter)
                .damageDealt(damageDealt)
                .opponentDamageDealt(opponentDamageDealt)
                .fightStartedAt(fightStartedAt)
                .fightDurationSec(fightDurationSec);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OlympiadMatchResultEvent)) return false;
        OlympiadMatchResultEvent that = (OlympiadMatchResultEvent) o;
        return olympiadCycle == that.olympiadCycle
                && charId == that.charId
                && classId == that.classId
                && opponentCharId == that.opponentCharId
                && opponentClassId == that.opponentClassId
                && pointsBefore == that.pointsBefore
                && pointsAfter == that.pointsAfter
                && damageDealt == that.damageDealt
                && opponentDamageDealt == that.opponentDamageDealt
                && fightDurationSec == that.fightDurationSec
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(matchId, that.matchId)
                && gameType == that.gameType
                && Objects.equals(clanId, that.clanId)
                && Objects.equals(opponentClanId, that.opponentClanId)
                && result == that.result
                && reason == that.reason
                && Objects.equals(fightStartedAt, that.fightStartedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, matchId, olympiadCycle, gameType,
                charId, classId, clanId,
                opponentCharId, opponentClassId, opponentClanId,
                result, reason,
                pointsBefore, pointsAfter,
                damageDealt, opponentDamageDealt,
                fightStartedAt, fightDurationSec);
    }

    @Override
    public String toString() {
        return "OlympiadMatchResultEvent[eventId=" + eventId
                + ", matchId=" + matchId
                + ", olympiadCycle=" + olympiadCycle
                + ", gameType=" + gameType
                + ", charId=" + charId
                + ", classId=" + classId
                + ", clanId=" + clanId
                + ", opponentCharId=" + opponentCharId
                + ", opponentClassId=" + opponentClassId
                + ", opponentClanId=" + opponentClanId
                + ", result=" + result
                + ", reason=" + reason
                + ", pointsBefore=" + pointsBefore
                + ", pointsAfter=" + pointsAfter
                + ", damageDealt=" + damageDealt
                + ", opponentDamageDealt=" + opponentDamageDealt
                + ", fightStartedAt=" + fightStartedAt
                + ", fightDurationSec=" + fightDurationSec + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private UUID matchId;
        private int olympiadCycle;
        private OlympiadGameType gameType;
        private long charId;
        private int classId;
        private @Nullable Long clanId;
        private long opponentCharId;
        private int opponentClassId;
        private @Nullable Long opponentClanId;
        private OlympiadMatchResult result;
        private OlympiadMatchReason reason;
        private int pointsBefore;
        private int pointsAfter;
        private int damageDealt;
        private int opponentDamageDealt;
        private @Nullable Instant fightStartedAt;
        private long fightDurationSec;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder matchId(UUID matchId) {
            this.matchId = matchId;
            return this;
        }

        public Builder olympiadCycle(int olympiadCycle) {
            this.olympiadCycle = olympiadCycle;
            return this;
        }

        public Builder gameType(OlympiadGameType gameType) {
            this.gameType = gameType;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder classId(int classId) {
            this.classId = classId;
            return this;
        }

        public Builder clanId(@Nullable Long clanId) {
            this.clanId = clanId;
            return this;
        }

        public Builder opponentCharId(long opponentCharId) {
            this.opponentCharId = opponentCharId;
            return this;
        }

        public Builder opponentClassId(int opponentClassId) {
            this.opponentClassId = opponentClassId;
            return this;
        }

        public Builder opponentClanId(@Nullable Long opponentClanId) {
            this.opponentClanId = opponentClanId;
            return this;
        }

        public Builder result(OlympiadMatchResult result) {
            this.result = result;
            return this;
        }

        public Builder reason(OlympiadMatchReason reason) {
            this.reason = reason;
            return this;
        }

        public Builder pointsBefore(int pointsBefore) {
            this.pointsBefore = pointsBefore;
            return this;
        }

        public Builder pointsAfter(int pointsAfter) {
            this.pointsAfter = pointsAfter;
            return this;
        }

        public Builder damageDealt(int damageDealt) {
            this.damageDealt = damageDealt;
            return this;
        }

        public Builder opponentDamageDealt(int opponentDamageDealt) {
            this.opponentDamageDealt = opponentDamageDealt;
            return this;
        }

        public Builder fightStartedAt(@Nullable Instant fightStartedAt) {
            this.fightStartedAt = fightStartedAt;
            return this;
        }

        public Builder fightDurationSec(long fightDurationSec) {
            this.fightDurationSec = fightDurationSec;
            return this;
        }

        public OlympiadMatchResultEvent build() {
            return new OlympiadMatchResultEvent(eventId, matchId, olympiadCycle, gameType,
                    charId, classId, clanId,
                    opponentCharId, opponentClassId, opponentClanId,
                    result, reason,
                    pointsBefore, pointsAfter,
                    damageDealt, opponentDamageDealt,
                    fightStartedAt, fightDurationSec);
        }
    }
}
