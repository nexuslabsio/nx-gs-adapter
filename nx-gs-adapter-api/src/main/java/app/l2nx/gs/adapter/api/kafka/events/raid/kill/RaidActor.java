package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * Snapshot of a single character's identity + affiliation + damage
 * contribution at a raid event moment. Used for
 * {@link RaidKillEvent#getLastHit() lastHit},
 * {@link RaidKillEvent#getDropOwner() dropOwner}, and every entry of
 * {@link RaidKillEvent#getParticipants() participants}.
 *
 * <p>Identity-affiliation fields ({@code clanId} / {@code allyId} /
 * {@code partyId} / {@code commandChannelId}) are captured at the event
 * moment — a character can switch clans / parties mid-fight; only the
 * event-moment value is recorded.</p>
 *
 * <p>{@code partyId} / {@code commandChannelId} are UUIDv7 identifiers minted
 * by the host on Party / CommandChannel construction. Stable across leader
 * changes within the same in-memory group; reset on disband / server restart.</p>
 *
 * <p>{@link #isPartyLeader() partyLeader} / {@link #isCommandChannelLeader()
 * commandChannelLeader} are required booleans capturing whether this actor
 * was the leader of their Party / CommandChannel at the event moment. Both
 * are {@code false} when the actor has no party / no CC. The "leader party"
 * of a CC is the party whose leader is also the CC leader.</p>
 *
 * <p>{@link #getDamageDealt() damageDealt} is the actor's accumulated damage
 * to this raid (from the host aggro list). Always {@code >= 0}; {@code 0}
 * is a valid value in {@link RaidKillEvent#getParticipants() participants}
 * — surfaces healers / tanks / aggro-skill users who only accrued hate, and
 * Party / CommandChannel teammates of active damagers (pure buffers).
 * Also valid for {@link RaidKillEvent#getLastHit() lastHit} when the final
 * blow came with the boss already at 0 HP from someone else's damage, or
 * for a GM {@code //kill} where the killer never aggroed. Names (char /
 * clan) are intentionally NOT carried — the platform joins on
 * {@code charId} / {@code clanId} against the CDC-synced character / clan
 * catalogs.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
 */
public final class RaidActor {

    private final long charId;
    private final @Nullable Long clanId;
    private final @Nullable Long allyId;
    private final @Nullable UUID partyId;
    private final @Nullable UUID commandChannelId;
    private final boolean partyLeader;
    private final boolean commandChannelLeader;
    private final long damageDealt;

    public RaidActor(long charId,
                     @Nullable Long clanId,
                     @Nullable Long allyId,
                     @Nullable UUID partyId,
                     @Nullable UUID commandChannelId,
                     boolean partyLeader,
                     boolean commandChannelLeader,
                     long damageDealt) {
        this.charId = charId;
        this.clanId = clanId;
        this.allyId = allyId;
        this.partyId = partyId;
        this.commandChannelId = commandChannelId;
        this.partyLeader = partyLeader;
        this.commandChannelLeader = commandChannelLeader;
        this.damageDealt = damageDealt;
    }

    /**
     * Character {@code objectId}.
     */
    public long getCharId() {
        return charId;
    }

    /**
     * Clan affiliation; {@code null} when unaffiliated.
     */
    public @Nullable Long getClanId() {
        return clanId;
    }

    /**
     * Alliance affiliation; {@code null} when unaffiliated.
     */
    public @Nullable Long getAllyId() {
        return allyId;
    }

    /**
     * Party identity (UUIDv7); {@code null} when solo at the event moment.
     */
    public @Nullable UUID getPartyId() {
        return partyId;
    }

    /**
     * Command channel identity (UUIDv7); {@code null} when the actor's party
     * was not in a CC at the event moment.
     */
    public @Nullable UUID getCommandChannelId() {
        return commandChannelId;
    }

    /**
     * {@code true} ⇔ this actor was the leader of their Party at the event
     * moment. {@code false} when the actor has no party at the moment.
     */
    public boolean isPartyLeader() {
        return partyLeader;
    }

    /**
     * {@code true} ⇔ this actor was the leader of their CommandChannel at
     * the event moment. {@code false} when the actor has no CC at the moment.
     */
    public boolean isCommandChannelLeader() {
        return commandChannelLeader;
    }

    /**
     * Accumulated damage from the host aggro list. {@code >= 0}; {@code 0}
     * is valid (KS final blow with prior damage by others, GM kill, etc.).
     */
    public long getDamageDealt() {
        return damageDealt;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .clanId(clanId)
                .allyId(allyId)
                .partyId(partyId)
                .commandChannelId(commandChannelId)
                .partyLeader(partyLeader)
                .commandChannelLeader(commandChannelLeader)
                .damageDealt(damageDealt);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaidActor)) return false;
        RaidActor that = (RaidActor) o;
        return charId == that.charId
                && damageDealt == that.damageDealt
                && partyLeader == that.partyLeader
                && commandChannelLeader == that.commandChannelLeader
                && Objects.equals(clanId, that.clanId)
                && Objects.equals(allyId, that.allyId)
                && Objects.equals(partyId, that.partyId)
                && Objects.equals(commandChannelId, that.commandChannelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, clanId, allyId, partyId, commandChannelId,
                partyLeader, commandChannelLeader, damageDealt);
    }

    @Override
    public String toString() {
        return "RaidActor[charId=" + charId
                + ", clanId=" + clanId
                + ", allyId=" + allyId
                + ", partyId=" + partyId
                + ", commandChannelId=" + commandChannelId
                + ", partyLeader=" + partyLeader
                + ", commandChannelLeader=" + commandChannelLeader
                + ", damageDealt=" + damageDealt + "]";
    }

    public static final class Builder {
        private long charId;
        private @Nullable Long clanId;
        private @Nullable Long allyId;
        private @Nullable UUID partyId;
        private @Nullable UUID commandChannelId;
        private boolean partyLeader;
        private boolean commandChannelLeader;
        private long damageDealt;

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder clanId(@Nullable Long clanId) {
            this.clanId = clanId;
            return this;
        }

        public Builder allyId(@Nullable Long allyId) {
            this.allyId = allyId;
            return this;
        }

        public Builder partyId(@Nullable UUID partyId) {
            this.partyId = partyId;
            return this;
        }

        public Builder commandChannelId(@Nullable UUID commandChannelId) {
            this.commandChannelId = commandChannelId;
            return this;
        }

        public Builder partyLeader(boolean partyLeader) {
            this.partyLeader = partyLeader;
            return this;
        }

        public Builder commandChannelLeader(boolean commandChannelLeader) {
            this.commandChannelLeader = commandChannelLeader;
            return this;
        }

        public Builder damageDealt(long damageDealt) {
            this.damageDealt = damageDealt;
            return this;
        }

        public RaidActor build() {
            return new RaidActor(charId, clanId, allyId, partyId, commandChannelId,
                    partyLeader, commandChannelLeader, damageDealt);
        }
    }
}
