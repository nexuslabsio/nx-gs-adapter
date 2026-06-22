package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import app.l2nx.gs.adapter.api.kafka.events.raid.RaidBossKind;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO published to the {@code raid} family topic
 * ({@code <tenant>.gs.events.raid}) when an {@code Attackable.isRaid() &&
 * !isRaidMinion()} target dies. One event per kill, regardless of fight
 * scale — a Valakas raid involving multiple command channels and a daily
 * instance boss killed by one party emit the same shape.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire kill-timestamp
 * is encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code killedAt} field. Platform consumers dedupe on {@code eventId}
 * (at-least-once delivery). Partition key on the wire is the
 * {@link #getBossNpcId() bossNpcId} (8-byte big-endian) — per-boss kill
 * history lands on one partition for ordered consumption.</p>
 *
 * <p>Three {@link RaidActor} reference points share one shape (charId +
 * affiliation + per-raid damage contribution):
 * <ul>
 *   <li>{@link #getLastHit() lastHit} — final-blow character. {@code null}
 *   when the last hit came from a non-player source (trap, owner-less
 *   summon, kill curse). Narrative only — does NOT confer drop rights in L2.</li>
 *   <li>{@link #getDropOwner() dropOwner} — host-side
 *   {@code mainDamageDealer} who received drop protection on the rolled
 *   loot. Group-first semantics: when {@code dropOwner.partyId} is non-null,
 *   drop rights belong to that party (consumer aggregates analytics by
 *   {@code partyId} / {@code commandChannelId}), and {@code dropOwner.charId}
 *   identifies the L2 representative of that group (may differ across kills
 *   of the same party — do NOT aggregate by it). When
 *   {@code dropOwner.partyId} is null the kill was solo and
 *   {@code dropOwner.charId} IS the drop owner directly.
 *   {@code null} when no resolvable player damager (admin kill, instant
 *   kill with empty aggro list).</li>
 *   <li>{@link #getParticipants() participants} — one
 *   {@code RaidActor} per character who took part in the kill. A character
 *   qualifies via the host aggro list (damage <em>or</em> hate accrued —
 *   the latter covers healers / tanks / aggro-skill users who never landed
 *   their own damage) <em>or</em> via Party / CommandChannel membership of
 *   any aggro-list contributor (covers pure buffers grouped with the actual
 *   fighters). Producers SHOULD emit sorted by
 *   {@link RaidActor#getDamageDealt() damageDealt} desc as a consumer
 *   convenience — support entries (damage=0) park at the tail.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about this kill. {@code null} when absent;
 *   hosts MAY add arbitrary keys without an API release and consumers
 *   ignore keys they do not understand.</li>
 * </ul></p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
 */
public final class RaidKillEvent {

    private final UUID eventId;
    private final int bossNpcId;
    private final @Nullable String bossName;
    private final @Nullable Integer bossLevel;
    private final RaidBossKind bossKind;
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
            RaidBossKind bossKind,
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
        this.bossKind = Objects.requireNonNull(bossKind, "RaidKillEvent.bossKind is required");
        this.instanceId = instanceId;
        this.lastHit = lastHit;
        this.dropOwner = dropOwner;
        this.participants = freezeList(participants);
        this.drops = freezeList(drops);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the kill
     * occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * L2 NPC template id of the raid (e.g. {@code 29028} = Valakas).
     */
    public int getBossNpcId() {
        return bossNpcId;
    }

    /**
     * Display name at kill time; optional, platform can resolve from its
     * NPC catalog when {@code null}.
     */
    public @Nullable String getBossName() {
        return bossName;
    }

    /**
     * Boss level at spawn; optional.
     */
    public @Nullable Integer getBossLevel() {
        return bossLevel;
    }

    /**
     * Coarse classification — see {@link RaidBossKind} for detection rules.
     */
    public RaidBossKind getBossKind() {
        return bossKind;
    }

    /**
     * Instance world id when killed inside a reflection / instance zone;
     * {@code null} for open-world kills.
     */
    public @Nullable Long getInstanceId() {
        return instanceId;
    }

    /**
     * Final-blow character. {@code null} when the last hit came from a
     * non-player source.
     */
    public @Nullable RaidActor getLastHit() {
        return lastHit;
    }

    /**
     * Drop-rights actor — group-first semantics, see class-level Javadoc.
     */
    public @Nullable RaidActor getDropOwner() {
        return dropOwner;
    }

    /**
     * Per-character contribution records — one {@link RaidActor} per
     * character who fought the raid. Includes everyone with damage or
     * hate on the aggro list plus their Party / CommandChannel teammates
     * (supports who buffed without engaging directly). Always non-null on
     * read; {@code null} passed to the constructor is normalized to an
     * empty list.
     */
    public List<RaidActor> getParticipants() {
        return participants;
    }

    /**
     * Per-drop records — what the raid actually rolled. Always non-null on
     * read; {@code null} passed to the constructor is normalized to an empty
     * list.
     */
    public List<RaidDropItem> getDrops() {
        return drops;
    }

    /**
     * Optional open string→string attributes about this kill — {@code null}
     * when absent. Hosts MAY add arbitrary keys without
     * an API release; consumers ignore keys they do not understand.
     */
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

        public Builder bossKind(RaidBossKind bossKind) {
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
