package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import app.l2nx.gs.adapter.api.kafka.events.raid.model.RaidBossKind;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Published to the {@code raid} topic when a non-minion raid boss dies, one event per kill. {@code eventId}
 * MUST be a UUIDv7 (consumers dedupe on it, at-least-once); the partition key is {@code bossNpcId}.
 *
 * <ul>
 *   <li>{@code lastHit} - final-blow character; does NOT confer drop rights.</li>
 *   <li>{@code dropOwner} - group-first: when {@code partyId} is non-null the party owns the drop and
 *   {@code charId} is only its representative (do NOT aggregate by it).</li>
 *   <li>{@code participants} - aggro-list characters plus party / command-channel teammates of any contributor;
 *   producers SHOULD sort by {@code damageDealt} desc.</li>
 * </ul>
 */
public final class RaidKillEvent {

    private final UUID eventId;
    private final int bossNpcId;
    private final @Nullable String bossName;
    private final @Nullable Integer bossLevel;
    private final @Nullable RaidBossKind bossKind;
    private final @Nullable Long instanceId;
    private final @Nullable RaidActor lastHit;
    private final @Nullable RaidActor dropOwner;
    private final List<RaidActor> participants;
    private final List<RaidDropItem> drops;
    private final @Nullable Map<String, String> metadata;

    public RaidKillEvent(
            UUID eventId,
            int bossNpcId,
            @Nullable String bossName,
            @Nullable Integer bossLevel,
            @Nullable RaidBossKind bossKind,
            @Nullable Long instanceId,
            @Nullable RaidActor lastHit,
            @Nullable RaidActor dropOwner,
            @Nullable List<RaidActor> participants,
            @Nullable List<RaidDropItem> drops,
            @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "RaidKillEvent.eventId is required");
        this.bossNpcId = bossNpcId;
        this.bossName = bossName;
        this.bossLevel = bossLevel;
        this.bossKind = bossKind;
        this.instanceId = instanceId;
        this.lastHit = lastHit;
        this.dropOwner = dropOwner;
        this.participants = freezeList(participants);
        this.drops = freezeList(drops);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /** MUST be a UUIDv7. */
    public UUID getEventId() {
        return eventId;
    }

    public int getBossNpcId() {
        return bossNpcId;
    }

    /** {@code null}: platform resolves from its NPC catalog. */
    public @Nullable String getBossName() {
        return bossName;
    }

    public @Nullable Integer getBossLevel() {
        return bossLevel;
    }

    /**
     * @deprecated the platform no longer reads it; use the NPC template {@code type} instead.
     * {@code null} when the host omits it.
     */
    @Deprecated
    // TODO: remove once all hosts run this adapter version and the raid topic is drained
    public @Nullable RaidBossKind getBossKind() {
        return bossKind;
    }

    /** {@code null} for open-world kills. */
    public @Nullable Long getInstanceId() {
        return instanceId;
    }

    /** {@code null} when the last hit came from a non-player source. */
    public @Nullable RaidActor getLastHit() {
        return lastHit;
    }

    public @Nullable RaidActor getDropOwner() {
        return dropOwner;
    }

    /** Never null; a {@code null} constructor argument becomes an empty list. */
    public List<RaidActor> getParticipants() {
        return participants;
    }

    /** Never null; a {@code null} constructor argument becomes an empty list. */
    public List<RaidDropItem> getDrops() {
        return drops;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .bossNpcId(bossNpcId)
                .bossName(bossName)
                .bossLevel(bossLevel)
                .bossKind(bossKind)
                .instanceId(instanceId)
                .lastHit(lastHit)
                .dropOwner(dropOwner)
                .participants(participants)
                .drops(drops)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static <T> List<T> freezeList(@Nullable List<T> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<T>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaidKillEvent)) return false;
        RaidKillEvent that = (RaidKillEvent) o;
        return bossNpcId == that.bossNpcId
                && eventId.equals(that.eventId)
                && Objects.equals(bossName, that.bossName)
                && Objects.equals(bossLevel, that.bossLevel)
                && bossKind == that.bossKind
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(lastHit, that.lastHit)
                && Objects.equals(dropOwner, that.dropOwner)
                && participants.equals(that.participants)
                && drops.equals(that.drops)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                eventId,
                bossNpcId,
                bossName,
                bossLevel,
                bossKind,
                instanceId,
                lastHit,
                dropOwner,
                participants,
                drops,
                metadata);
    }

    @Override
    public String toString() {
        return "RaidKillEvent[eventId=" + eventId
                + ", bossNpcId=" + bossNpcId
                + ", bossName=" + bossName
                + ", bossKind=" + bossKind
                + ", instanceId=" + instanceId
                + ", lastHit=" + lastHit
                + ", dropOwner=" + dropOwner
                + ", participants=" + participants.size()
                + ", drops=" + drops.size()
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private int bossNpcId;
        private @Nullable String bossName;
        private @Nullable Integer bossLevel;
        private @Nullable RaidBossKind bossKind;
        private @Nullable Long instanceId;
        private @Nullable RaidActor lastHit;
        private @Nullable RaidActor dropOwner;
        private @Nullable List<RaidActor> participants;
        private @Nullable List<RaidDropItem> drops;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder bossNpcId(int bossNpcId) {
            this.bossNpcId = bossNpcId;
            return this;
        }

        public Builder bossName(@Nullable String bossName) {
            this.bossName = bossName;
            return this;
        }

        public Builder bossLevel(@Nullable Integer bossLevel) {
            this.bossLevel = bossLevel;
            return this;
        }

        /**
         * @deprecated see {@link RaidKillEvent#getBossKind()}.
         */
        @Deprecated
        // TODO: remove once all hosts run this adapter version and the raid topic is drained
        public Builder bossKind(@Nullable RaidBossKind bossKind) {
            this.bossKind = bossKind;
            return this;
        }

        public Builder instanceId(@Nullable Long instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder lastHit(@Nullable RaidActor lastHit) {
            this.lastHit = lastHit;
            return this;
        }

        public Builder dropOwner(@Nullable RaidActor dropOwner) {
            this.dropOwner = dropOwner;
            return this;
        }

        public Builder participants(@Nullable List<RaidActor> participants) {
            this.participants = participants;
            return this;
        }

        public Builder drops(@Nullable List<RaidDropItem> drops) {
            this.drops = drops;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public RaidKillEvent build() {
            return new RaidKillEvent(
                    eventId,
                    bossNpcId,
                    bossName,
                    bossLevel,
                    bossKind,
                    instanceId,
                    lastHit,
                    dropOwner,
                    participants,
                    drops,
                    metadata);
        }
    }
}
