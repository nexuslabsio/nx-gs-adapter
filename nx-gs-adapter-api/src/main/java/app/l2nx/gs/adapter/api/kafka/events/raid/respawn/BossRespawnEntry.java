package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

import app.l2nx.gs.adapter.api.kafka.events.raid.RaidBossKind;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One tracked raid boss inside a {@link BossRespawnSnapshotEvent}. Describes the
 * boss's current status and, when it is dead, the moment it is scheduled to
 * respawn.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getNpcId() npcId} — REQUIRED. L2 NPC template id; the stable
 *   per-boss key the consumer upserts on. The consumer resolves the boss name
 *   from this id against its own NPC catalog — names are intentionally NOT
 *   carried on the wire.</li>
 *   <li>{@link #getLevel() level} — optional boss level.</li>
 *   <li>{@link #getKind() kind} — REQUIRED. Reuses {@link RaidBossKind}; this
 *   snapshot only ever carries {@link RaidBossKind#RAID RAID} (open-world raid
 *   boss) or {@link RaidBossKind#GRAND_BOSS GRAND_BOSS} (epic). Instance bosses
 *   are excluded — they have no server-wide respawn timer.</li>
 *   <li>{@link #getStatus() status} — REQUIRED. Open build-agnostic status
 *   string; canonical values in {@link WellKnownBossStatuses}
 *   ({@code alive} / {@code in_combat} / {@code dead}). Hosts MAY emit additional
 *   non-canonical statuses; consumers map unknown values to "not dead".</li>
 *   <li>{@link #getNextRespawnAt() nextRespawnAt} — optional. Instant of the
 *   next scheduled respawn. Set when {@link #getStatus() status} is
 *   {@link WellKnownBossStatuses#DEAD dead} and the respawn time is known;
 *   {@code null} when the boss is up, or dead with an unknown / unscheduled
 *   respawn.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic per-boss attributes. {@code null} when absent. Hosts MAY
 *   publish arbitrary keys without an API release; consumers ignore keys they
 *   do not understand.</li>
 * </ul>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
 */
public final class BossRespawnEntry {

    private final int npcId;
    private final @Nullable Integer level;
    private final RaidBossKind kind;
    private final String status;
    private final @Nullable Instant nextRespawnAt;
    private final @Nullable Map<String, String> metadata;

    public BossRespawnEntry(int npcId,
                            @Nullable Integer level,
                            RaidBossKind kind,
                            String status,
                            @Nullable Instant nextRespawnAt,
                            @Nullable Map<String, String> metadata) {
        this.npcId = npcId;
        this.level = level;
        this.kind = kind;
        this.status = status;
        this.nextRespawnAt = nextRespawnAt;
        this.metadata = metadata == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * L2 NPC template id. The stable per-boss key the platform upserts on inside
     * a snapshot and resolves the boss name from.
     */
    public int getNpcId() {
        return npcId;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    /**
     * Coarse classification — {@link RaidBossKind#RAID} or
     * {@link RaidBossKind#GRAND_BOSS} for this snapshot.
     */
    public RaidBossKind getKind() {
        return kind;
    }

    /**
     * Build-agnostic boss status — see {@link WellKnownBossStatuses} for the
     * canonical {@code alive} / {@code in_combat} / {@code dead} values.
     */
    public String getStatus() {
        return status;
    }

    /**
     * Instant of the next scheduled respawn, or {@code null} when the boss is up
     * or its respawn is unknown / unscheduled.
     */
    public @Nullable Instant getNextRespawnAt() {
        return nextRespawnAt;
    }

    /**
     * Open string→string map of build-agnostic per-boss attributes, or
     * {@code null} when absent. When non-null the returned map is unmodifiable.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .npcId(npcId)
                .level(level)
                .kind(kind)
                .status(status)
                .nextRespawnAt(nextRespawnAt)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BossRespawnEntry)) return false;
        BossRespawnEntry that = (BossRespawnEntry) o;
        return npcId == that.npcId
                && Objects.equals(level, that.level)
                && kind == that.kind
                && Objects.equals(status, that.status)
                && Objects.equals(nextRespawnAt, that.nextRespawnAt)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(npcId, level, kind, status, nextRespawnAt, metadata);
    }

    @Override
    public String toString() {
        return "BossRespawnEntry[npcId=" + npcId
                + ", level=" + level
                + ", kind=" + kind
                + ", status=" + status
                + ", nextRespawnAt=" + nextRespawnAt
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private int npcId;
        private @Nullable Integer level;
        private @Nullable RaidBossKind kind;
        private @Nullable String status;
        private @Nullable Instant nextRespawnAt;
        private @Nullable Map<String, String> metadata;

        public Builder npcId(int npcId) {
            this.npcId = npcId;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        public Builder kind(RaidBossKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder nextRespawnAt(@Nullable Instant nextRespawnAt) {
            this.nextRespawnAt = nextRespawnAt;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public BossRespawnEntry build() {
            return new BossRespawnEntry(npcId, level, kind, status, nextRespawnAt, metadata);
        }
    }
}
