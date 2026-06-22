package app.l2nx.gs.adapter.api.kafka.events.olympiad;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.time.Instant;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Closed Olympiad 1v1 match — one event per participant (self-perspective).
 * Both per-participant events share {@link #getMatchId() matchId}.
 *
 * <p>Emitted for every points-changing conclusion including no-fight edge
 * cases (default / disconnect / timeout / both-offline) — see
 * {@link OlympiadMatchReason}. {@link #getFightStartedAt() fightStartedAt}
 * is {@code null} when no combat occurred.</p>
 *
 * <p>{@link #getMetadata() metadata} — optional open string&rarr;string map of
 * build-agnostic attributes about this match. {@code null} when absent;
 * hosts MAY publish arbitrary keys without an API release, and
 * consumers ignore keys they do not understand.</p>
 */
public final class OlympiadMatchResultEvent {

    private final UUID eventId;
    private final UUID matchId;
    private final int olympiadCycle;
    private final OlympiadGameType gameType;

    private final long charId;
    private final int classId;
    private final @Nullable CharacterClass clazz;
    private final @Nullable Long clanId;

    private final long opponentCharId;
    private final int opponentClassId;
    private final @Nullable CharacterClass opponentClazz;
    private final @Nullable Long opponentClanId;

    private final OlympiadMatchResult result;
    private final OlympiadMatchReason reason;

    private final int pointsBefore;
    private final int pointsAfter;

    private final int damageDealt;
    private final int opponentDamageDealt;
    private final @Nullable Instant fightStartedAt;
    private final long fightDurationSec;
    private final @Nullable Map<String, String> metadata;

    public OlympiadMatchResultEvent(
            UUID eventId,
            UUID matchId,
            int olympiadCycle,
            OlympiadGameType gameType,
            long charId,
            int classId,
            @Nullable CharacterClass clazz,
            @Nullable Long clanId,
            long opponentCharId,
            int opponentClassId,
            @Nullable CharacterClass opponentClazz,
            @Nullable Long opponentClanId,
            OlympiadMatchResult result,
            OlympiadMatchReason reason,
            int pointsBefore,
            int pointsAfter,
            int damageDealt,
            int opponentDamageDealt,
            @Nullable Instant fightStartedAt,
            long fightDurationSec,
            @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.matchId = matchId;
        this.olympiadCycle = olympiadCycle;
        this.gameType = gameType;
        this.charId = charId;
        this.classId = classId;
        this.clazz = clazz;
        this.clanId = clanId;
        this.opponentCharId = opponentCharId;
        this.opponentClassId = opponentClassId;
        this.opponentClazz = opponentClazz;
        this.opponentClanId = opponentClanId;
        this.result = result;
        this.reason = reason;
        this.pointsBefore = pointsBefore;
        this.pointsAfter = pointsAfter;
        this.damageDealt = damageDealt;
        this.opponentDamageDealt = opponentDamageDealt;
        this.fightStartedAt = fightStartedAt;
        this.fightDurationSec = fightDurationSec;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
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

    /**
     * Legacy numeric class id (host numbering). Superseded by
     * {@link #getClazz() clazz}; retained for back-compat while hosts migrate to
     * the canonical token. Consumers MUST prefer {@code clazz} when it is non-null.
     */
    public int getClassId() {
        return classId;
    }

    /**
     * Canonical, source-agnostic class token. {@code null} from hosts that have
     * not yet migrated off the numeric {@link #getClassId() classId} (consumers
     * fall back to it then), or when the source class is not in the canonical
     * {@link CharacterClass} set.
     */
    public @Nullable CharacterClass getClazz() {
        return clazz;
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

    /**
     * Legacy numeric opponent class id (host numbering). Superseded by
     * {@link #getOpponentClazz() opponentClazz}; consumers MUST prefer the token
     * when it is non-null.
     */
    public int getOpponentClassId() {
        return opponentClassId;
    }

    /**
     * Canonical, source-agnostic opponent class token. {@code null} pre-migration
     * (fall back to {@link #getOpponentClassId() opponentClassId}) or for a
     * non-canonical source class.
     */
    public @Nullable CharacterClass getOpponentClazz() {
        return opponentClazz;
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

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .matchId(matchId)
                .olympiadCycle(olympiadCycle)
                .gameType(gameType)
                .charId(charId)
                .classId(classId)
                .clazz(clazz)
                .clanId(clanId)
                .opponentCharId(opponentCharId)
                .opponentClassId(opponentClassId)
                .opponentClazz(opponentClazz)
                .opponentClanId(opponentClanId)
                .result(result)
                .reason(reason)
                .pointsBefore(pointsBefore)
                .pointsAfter(pointsAfter)
                .damageDealt(damageDealt)
                .opponentDamageDealt(opponentDamageDealt)
                .fightStartedAt(fightStartedAt)
                .fightDurationSec(fightDurationSec)
                .metadata(metadata);
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
                && clazz == that.clazz
                && opponentCharId == that.opponentCharId
                && opponentClassId == that.opponentClassId
                && opponentClazz == that.opponentClazz
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
                && Objects.equals(fightStartedAt, that.fightStartedAt)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                eventId,
                matchId,
                olympiadCycle,
                gameType,
                charId,
                classId,
                clazz,
                clanId,
                opponentCharId,
                opponentClassId,
                opponentClazz,
                opponentClanId,
                result,
                reason,
                pointsBefore,
                pointsAfter,
                damageDealt,
                opponentDamageDealt,
                fightStartedAt,
                fightDurationSec,
                metadata);
    }

    @Override
    public String toString() {
        return "OlympiadMatchResultEvent[eventId=" + eventId
                + ", matchId=" + matchId
                + ", olympiadCycle=" + olympiadCycle
                + ", gameType=" + gameType
                + ", charId=" + charId
                + ", classId=" + classId
                + ", clazz=" + clazz
                + ", clanId=" + clanId
                + ", opponentCharId=" + opponentCharId
                + ", opponentClassId=" + opponentClassId
                + ", opponentClazz=" + opponentClazz
                + ", opponentClanId=" + opponentClanId
                + ", result=" + result
                + ", reason=" + reason
                + ", pointsBefore=" + pointsBefore
                + ", pointsAfter=" + pointsAfter
                + ", damageDealt=" + damageDealt
                + ", opponentDamageDealt=" + opponentDamageDealt
                + ", fightStartedAt=" + fightStartedAt
                + ", fightDurationSec=" + fightDurationSec
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private UUID matchId;
        private int olympiadCycle;
        private OlympiadGameType gameType;
        private long charId;
        private int classId;
        private @Nullable CharacterClass clazz;
        private @Nullable Long clanId;
        private long opponentCharId;
        private int opponentClassId;
        private @Nullable CharacterClass opponentClazz;
        private @Nullable Long opponentClanId;
        private OlympiadMatchResult result;
        private OlympiadMatchReason reason;
        private int pointsBefore;
        private int pointsAfter;
        private int damageDealt;
        private int opponentDamageDealt;
        private @Nullable Instant fightStartedAt;
        private long fightDurationSec;
        private @Nullable Map<String, String> metadata;

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

        public Builder clazz(@Nullable CharacterClass clazz) {
            this.clazz = clazz;
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

        public Builder opponentClazz(@Nullable CharacterClass opponentClazz) {
            this.opponentClazz = opponentClazz;
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

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public OlympiadMatchResultEvent build() {
            return new OlympiadMatchResultEvent(
                    eventId,
                    matchId,
                    olympiadCycle,
                    gameType,
                    charId,
                    classId,
                    clazz,
                    clanId,
                    opponentCharId,
                    opponentClassId,
                    opponentClazz,
                    opponentClanId,
                    result,
                    reason,
                    pointsBefore,
                    pointsAfter,
                    damageDealt,
                    opponentDamageDealt,
                    fightStartedAt,
                    fightDurationSec,
                    metadata);
        }
    }
}
