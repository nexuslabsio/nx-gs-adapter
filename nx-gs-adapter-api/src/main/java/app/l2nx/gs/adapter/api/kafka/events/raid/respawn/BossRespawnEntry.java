package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

import app.l2nx.gs.adapter.api.kafka.events.raid.model.RaidBossKind;
import app.l2nx.gs.adapter.api.kafka.events.schedule.RecurringSchedule;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One tracked raid boss in a {@link BossRespawnSnapshotEvent}.
 *
 * <ul>
 *   <li>{@code npcId} - upsert key; names are NOT on the wire.</li>
 *   <li>{@code kind} - deprecated and optional; when present only {@link RaidBossKind#RAID} or
 *   {@link RaidBossKind#EPIC}, as instance bosses have no server-wide respawn timer.</li>
 *   <li>{@code status} - open string ({@link WellKnownBossStatuses}); consumers map unknown values to "not dead".</li>
 *   <li>{@code nextRespawnAt} - set only when {@code dead} with a known respawn time.</li>
 * </ul>
 */
public final class BossRespawnEntry {

    private final int npcId;
    private final @Nullable Integer level;
    private final @Nullable RaidBossKind kind;
    private final String status;
    private final @Nullable Instant nextRespawnAt;
    private final @Nullable Map<String, String> metadata;
    private final @Nullable RecurringSchedule schedule;

    public BossRespawnEntry(
            int npcId,
            @Nullable Integer level,
            @Nullable RaidBossKind kind,
            String status,
            @Nullable Instant nextRespawnAt,
            @Nullable Map<String, String> metadata,
            @Nullable RecurringSchedule schedule) {
        this.npcId = npcId;
        this.level = level;
        this.kind = kind;
        this.status = status;
        this.nextRespawnAt = nextRespawnAt;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
        this.schedule = schedule;
    }

    public int getNpcId() {
        return npcId;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    /**
     * @deprecated the platform no longer reads it; use the NPC template {@code type} instead.
     * {@code null} when the host omits it.
     */
    @Deprecated
    // TODO: remove once all hosts run this adapter version and the raid topic is drained
    public @Nullable RaidBossKind getKind() {
        return kind;
    }

    public String getStatus() {
        return status;
    }

    public @Nullable Instant getNextRespawnAt() {
        return nextRespawnAt;
    }

    /** Unmodifiable when non-null. */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    /** {@code null} for respawn-window bosses and patterns that do not reduce to a weekly rule. */
    public @Nullable RecurringSchedule getSchedule() {
        return schedule;
    }

    public Builder toBuilder() {
        return new Builder()
                .npcId(npcId)
                .level(level)
                .kind(kind)
                .status(status)
                .nextRespawnAt(nextRespawnAt)
                .metadata(metadata)
                .schedule(schedule);
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
                && Objects.equals(metadata, that.metadata)
                && Objects.equals(schedule, that.schedule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(npcId, level, kind, status, nextRespawnAt, metadata, schedule);
    }

    @Override
    public String toString() {
        return "BossRespawnEntry[npcId=" + npcId
                + ", level=" + level
                + ", kind=" + kind
                + ", status=" + status
                + ", nextRespawnAt=" + nextRespawnAt
                + ", metadata=" + metadata
                + ", schedule=" + schedule + "]";
    }

    public static final class Builder {
        private int npcId;
        private @Nullable Integer level;
        private @Nullable RaidBossKind kind;
        private @Nullable String status;
        private @Nullable Instant nextRespawnAt;
        private @Nullable Map<String, String> metadata;
        private @Nullable RecurringSchedule schedule;

        public Builder npcId(int npcId) {
            this.npcId = npcId;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        /**
         * @deprecated see {@link BossRespawnEntry#getKind()}.
         */
        @Deprecated
        // TODO: remove once all hosts run this adapter version and the raid topic is drained
        public Builder kind(@Nullable RaidBossKind kind) {
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

        public Builder schedule(@Nullable RecurringSchedule schedule) {
            this.schedule = schedule;
            return this;
        }

        public BossRespawnEntry build() {
            return new BossRespawnEntry(npcId, level, kind, status, nextRespawnAt, metadata, schedule);
        }
    }
}
