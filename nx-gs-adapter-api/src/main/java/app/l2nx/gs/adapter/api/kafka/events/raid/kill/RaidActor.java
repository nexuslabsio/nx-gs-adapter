package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Character snapshot (identity, affiliation, damage) at the raid event moment; used for {@link RaidKillEvent#getLastHit() lastHit}, {@link RaidKillEvent#getDropOwner() dropOwner} and each {@link RaidKillEvent#getParticipants() participants} entry.
 * <p>Affiliations are event-moment values (characters can switch clans / parties mid-fight). {@code partyId} / {@code commandChannelId} are host-minted UUIDv7s, stable across leader changes, reset on disband / restart.
 * <p>{@code partyLeader} / {@code commandChannelLeader} are {@code false} without a party / CC; the "leader party" of a CC is the party whose leader is also the CC leader.
 * <p>{@link #getDamageDealt() damageDealt} comes from the host aggro list, {@code >= 0}; {@code 0} is valid (healers, tanks, pure buffers, a final blow on an already-dead boss, GM {@code //kill}). Names are not carried - join on charId / clanId via CDC.
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

    public RaidActor(
            long charId,
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

    public long getCharId() {
        return charId;
    }

    public @Nullable Long getClanId() {
        return clanId;
    }

    public @Nullable Long getAllyId() {
        return allyId;
    }

    /** {@code null} when solo at the event moment. */
    public @Nullable UUID getPartyId() {
        return partyId;
    }

    /** {@code null} when the actor's party was not in a CC. */
    public @Nullable UUID getCommandChannelId() {
        return commandChannelId;
    }

    public boolean isPartyLeader() {
        return partyLeader;
    }

    public boolean isCommandChannelLeader() {
        return commandChannelLeader;
    }

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
        return Objects.hash(
                charId, clanId, allyId, partyId, commandChannelId, partyLeader, commandChannelLeader, damageDealt);
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
            return new RaidActor(
                    charId, clanId, allyId, partyId, commandChannelId, partyLeader, commandChannelLeader, damageDealt);
        }
    }
}
