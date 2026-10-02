package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.Activity;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.WellKnownActivities;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.WellKnownActivityMetadata;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.WellKnownAiStatuses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Volatile runtime state of one character; payload of {@code SyncEvent<CharacterRuntimeDto>}. Shares {@code id}
 * with {@code CharacterDbDto}; only {@code id} is required.
 *
 * <p>{@code online} null/omitted means ONLINE; explicit {@code false} is a one-shot tombstone with everything else null.
 * An offline trader is not a tombstone: it ticks with {@code online=false} and an {@link WellKnownActivities#OFFLINE_TRADE} activity.</p>
 *
 * <p>{@code aiStatus} and {@code activities} are independent signals, both null on tombstones. Inventory and weight
 * caps ride this channel because they are stat-derived; consumers keep last-known values after logout.</p>
 */
public final class CharacterRuntimeDto {

    private final long id;
    private final @Nullable Integer curHp;
    private final @Nullable Integer maxHp;
    private final @Nullable Integer curMp;
    private final @Nullable Integer maxMp;
    private final @Nullable Integer curCp;
    private final @Nullable Integer maxCp;
    private final @Nullable Integer curVit;
    private final @Nullable Integer maxVit;
    private final @Nullable Integer x;
    private final @Nullable Integer y;
    private final @Nullable Integer z;
    private final @Nullable Boolean online;
    private final @Nullable String aiStatus;
    private final @Nullable CharacterClass classId;
    private final @Nullable Integer level;
    private final @Nullable Long exp;
    private final @Nullable Long sp;
    private final @Nullable List<Activity> activities;
    private final @Nullable Integer curInventorySlots;
    private final @Nullable Integer maxInventorySlots;
    private final @Nullable Integer curQuestInventorySlots;
    private final @Nullable Integer maxQuestInventorySlots;
    private final @Nullable Integer curWeight;
    private final @Nullable Integer maxWeight;

    /**
     * Prefer {@link #builder()}.
     *
     * <p>Must stay the only non-default constructor: the DTO binds by implicit parameter names, and an overload makes
     * creator detection ambiguous so consumers fail to deserialize the whole channel. Grow the wire by appending parameters.</p>
     */
    public CharacterRuntimeDto(
            long id,
            @Nullable Integer curHp,
            @Nullable Integer maxHp,
            @Nullable Integer curMp,
            @Nullable Integer maxMp,
            @Nullable Integer curCp,
            @Nullable Integer maxCp,
            @Nullable Integer curVit,
            @Nullable Integer maxVit,
            @Nullable Integer x,
            @Nullable Integer y,
            @Nullable Integer z,
            @Nullable Boolean online,
            @Nullable String aiStatus,
            @Nullable CharacterClass classId,
            @Nullable Integer level,
            @Nullable Long exp,
            @Nullable Long sp,
            @Nullable List<Activity> activities,
            @Nullable Integer curInventorySlots,
            @Nullable Integer maxInventorySlots,
            @Nullable Integer curQuestInventorySlots,
            @Nullable Integer maxQuestInventorySlots,
            @Nullable Integer curWeight,
            @Nullable Integer maxWeight) {
        this.id = id;
        this.curHp = curHp;
        this.maxHp = maxHp;
        this.curMp = curMp;
        this.maxMp = maxMp;
        this.curCp = curCp;
        this.maxCp = maxCp;
        this.curVit = curVit;
        this.maxVit = maxVit;
        this.x = x;
        this.y = y;
        this.z = z;
        this.online = online;
        this.aiStatus = aiStatus;
        this.classId = classId;
        this.level = level;
        this.exp = exp;
        this.sp = sp;
        this.activities = copy(activities);
        this.curInventorySlots = curInventorySlots;
        this.maxInventorySlots = maxInventorySlots;
        this.curQuestInventorySlots = curQuestInventorySlots;
        this.maxQuestInventorySlots = maxQuestInventorySlots;
        this.curWeight = curWeight;
        this.maxWeight = maxWeight;
    }

    private static @Nullable List<Activity> copy(@Nullable List<Activity> activities) {
        return activities == null ? null : Collections.unmodifiableList(new ArrayList<Activity>(activities));
    }

    public long getId() {
        return id;
    }

    /**
     * Whether this row carries observed state rather than being the offline tombstone (only {@code id} and
     * {@code online=false}). Not the same as {@link #getOnline() online}: offline traders carry real vitals.
     *
     * <p>Enumerates this class's own fields so a newly added volatile field gets added here; a missed field turns an
     * ordinary tick into a tombstone for every consumer.</p>
     */
    public boolean carriesState() {
        return curHp != null
                || maxHp != null
                || curMp != null
                || maxMp != null
                || curCp != null
                || maxCp != null
                || curVit != null
                || maxVit != null
                || x != null
                || y != null
                || z != null
                || aiStatus != null
                || classId != null
                || level != null
                || exp != null
                || sp != null
                || activities != null
                || curInventorySlots != null
                || maxInventorySlots != null
                || curQuestInventorySlots != null
                || maxQuestInventorySlots != null
                || curWeight != null
                || maxWeight != null;
    }

    public @Nullable Integer getCurHp() {
        return curHp;
    }

    public @Nullable Integer getMaxHp() {
        return maxHp;
    }

    public @Nullable Integer getCurMp() {
        return curMp;
    }

    public @Nullable Integer getMaxMp() {
        return maxMp;
    }

    public @Nullable Integer getCurCp() {
        return curCp;
    }

    public @Nullable Integer getMaxCp() {
        return maxCp;
    }

    /**
     * Null on cores without vitality.
     */
    public @Nullable Integer getCurVit() {
        return curVit;
    }

    public @Nullable Integer getMaxVit() {
        return maxVit;
    }

    public @Nullable Integer getX() {
        return x;
    }

    public @Nullable Integer getY() {
        return y;
    }

    public @Nullable Integer getZ() {
        return z;
    }

    /**
     * Null/omitted is read as ONLINE; see {@link #isOnlineEffective()}.
     */
    public @Nullable Boolean getOnline() {
        return online;
    }

    /**
     * Open string; canonical values in {@link WellKnownAiStatuses}. Null when unreported or on tombstones.
     */
    public @Nullable String getAiStatus() {
        return aiStatus;
    }

    /**
     * Class that {@link #getLevel()}, {@link #getExp()} and {@link #getSp()} describe (the subclass when one is active).
     * Null when the source ID is outside {@link CharacterClass} and on tombstones.
     */
    public @Nullable CharacterClass getClassId() {
        return classId;
    }

    /**
     * Level of the class named by {@link #getClassId()}, not necessarily the main class.
     */
    public @Nullable Integer getLevel() {
        return level;
    }

    public @Nullable Long getSp() {
        return sp;
    }

    /**
     * Absolute EXP total of the class named by {@link #getClassId()}, not a within-level delta.
     */
    public @Nullable Long getExp() {
        return exp;
    }

    /**
     * One slot per item stack, equipped items included; quest items are counted separately.
     */
    public @Nullable Integer getCurInventorySlots() {
        return curInventorySlots;
    }

    public @Nullable Integer getMaxInventorySlots() {
        return maxInventorySlots;
    }

    public @Nullable Integer getCurQuestInventorySlots() {
        return curQuestInventorySlots;
    }

    public @Nullable Integer getMaxQuestInventorySlots() {
        return maxQuestInventorySlots;
    }

    /**
     * Sum of {@code itemWeight * count} over all items including quest items, minus any build-specific penalty reduction.
     */
    public @Nullable Integer getCurWeight() {
        return curWeight;
    }

    public @Nullable Integer getMaxWeight() {
        return maxWeight;
    }

    /**
     * Null when there are none, the host does not report them, or on tombstones; otherwise unmodifiable.
     * Independent of {@link #getAiStatus() aiStatus}; metadata keys in {@link WellKnownActivityMetadata}.
     */
    public @Nullable List<Activity> getActivities() {
        return activities;
    }

    /**
     * True for null/true, false only for an explicit {@code false} tombstone.
     */
    public boolean isOnlineEffective() {
        return online == null || online;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .curHp(curHp)
                .maxHp(maxHp)
                .curMp(curMp)
                .maxMp(maxMp)
                .curCp(curCp)
                .maxCp(maxCp)
                .curVit(curVit)
                .maxVit(maxVit)
                .x(x)
                .y(y)
                .z(z)
                .online(online)
                .aiStatus(aiStatus)
                .classId(classId)
                .level(level)
                .exp(exp)
                .sp(sp)
                .activities(activities)
                .curInventorySlots(curInventorySlots)
                .maxInventorySlots(maxInventorySlots)
                .curQuestInventorySlots(curQuestInventorySlots)
                .maxQuestInventorySlots(maxQuestInventorySlots)
                .curWeight(curWeight)
                .maxWeight(maxWeight);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterRuntimeDto)) return false;
        CharacterRuntimeDto that = (CharacterRuntimeDto) o;
        return id == that.id
                && Objects.equals(curHp, that.curHp)
                && Objects.equals(maxHp, that.maxHp)
                && Objects.equals(curMp, that.curMp)
                && Objects.equals(maxMp, that.maxMp)
                && Objects.equals(curCp, that.curCp)
                && Objects.equals(maxCp, that.maxCp)
                && Objects.equals(curVit, that.curVit)
                && Objects.equals(maxVit, that.maxVit)
                && Objects.equals(x, that.x)
                && Objects.equals(y, that.y)
                && Objects.equals(z, that.z)
                && Objects.equals(online, that.online)
                && Objects.equals(aiStatus, that.aiStatus)
                && classId == that.classId
                && Objects.equals(level, that.level)
                && Objects.equals(exp, that.exp)
                && Objects.equals(sp, that.sp)
                && Objects.equals(activities, that.activities)
                && Objects.equals(curInventorySlots, that.curInventorySlots)
                && Objects.equals(maxInventorySlots, that.maxInventorySlots)
                && Objects.equals(curQuestInventorySlots, that.curQuestInventorySlots)
                && Objects.equals(maxQuestInventorySlots, that.maxQuestInventorySlots)
                && Objects.equals(curWeight, that.curWeight)
                && Objects.equals(maxWeight, that.maxWeight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                curHp,
                maxHp,
                curMp,
                maxMp,
                curCp,
                maxCp,
                curVit,
                maxVit,
                x,
                y,
                z,
                online,
                aiStatus,
                classId,
                level,
                exp,
                sp,
                activities,
                curInventorySlots,
                maxInventorySlots,
                curQuestInventorySlots,
                maxQuestInventorySlots,
                curWeight,
                maxWeight);
    }

    @Override
    public String toString() {
        return "CharacterRuntimeDto[id=" + id
                + ", curHp=" + curHp + ", maxHp=" + maxHp
                + ", curMp=" + curMp + ", maxMp=" + maxMp
                + ", curCp=" + curCp + ", maxCp=" + maxCp
                + ", curVit=" + curVit + ", maxVit=" + maxVit
                + ", x=" + x + ", y=" + y + ", z=" + z
                + ", online=" + online
                + ", aiStatus=" + aiStatus
                + ", classId=" + classId
                + ", level=" + level
                + ", exp=" + exp
                + ", sp=" + sp
                + ", activities=" + activities
                + ", curInventorySlots=" + curInventorySlots + ", maxInventorySlots=" + maxInventorySlots
                + ", curQuestInventorySlots=" + curQuestInventorySlots
                + ", maxQuestInventorySlots=" + maxQuestInventorySlots
                + ", curWeight=" + curWeight + ", maxWeight=" + maxWeight + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable Integer curHp;
        private @Nullable Integer maxHp;
        private @Nullable Integer curMp;
        private @Nullable Integer maxMp;
        private @Nullable Integer curCp;
        private @Nullable Integer maxCp;
        private @Nullable Integer curVit;
        private @Nullable Integer maxVit;
        private @Nullable Integer x;
        private @Nullable Integer y;
        private @Nullable Integer z;
        private @Nullable Boolean online;
        private @Nullable String aiStatus;
        private @Nullable CharacterClass classId;
        private @Nullable Integer level;
        private @Nullable Long exp;
        private @Nullable Long sp;
        private @Nullable List<Activity> activities;
        private @Nullable Integer curInventorySlots;
        private @Nullable Integer maxInventorySlots;
        private @Nullable Integer curQuestInventorySlots;
        private @Nullable Integer maxQuestInventorySlots;
        private @Nullable Integer curWeight;
        private @Nullable Integer maxWeight;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder curHp(@Nullable Integer curHp) {
            this.curHp = curHp;
            return this;
        }

        public Builder maxHp(@Nullable Integer maxHp) {
            this.maxHp = maxHp;
            return this;
        }

        public Builder curMp(@Nullable Integer curMp) {
            this.curMp = curMp;
            return this;
        }

        public Builder maxMp(@Nullable Integer maxMp) {
            this.maxMp = maxMp;
            return this;
        }

        public Builder curCp(@Nullable Integer curCp) {
            this.curCp = curCp;
            return this;
        }

        public Builder maxCp(@Nullable Integer maxCp) {
            this.maxCp = maxCp;
            return this;
        }

        public Builder curVit(@Nullable Integer curVit) {
            this.curVit = curVit;
            return this;
        }

        public Builder maxVit(@Nullable Integer maxVit) {
            this.maxVit = maxVit;
            return this;
        }

        public Builder x(@Nullable Integer x) {
            this.x = x;
            return this;
        }

        public Builder y(@Nullable Integer y) {
            this.y = y;
            return this;
        }

        public Builder z(@Nullable Integer z) {
            this.z = z;
            return this;
        }

        public Builder online(@Nullable Boolean online) {
            this.online = online;
            return this;
        }

        public Builder aiStatus(@Nullable String aiStatus) {
            this.aiStatus = aiStatus;
            return this;
        }

        public Builder exp(@Nullable Long exp) {
            this.exp = exp;
            return this;
        }

        public Builder classId(@Nullable CharacterClass classId) {
            this.classId = classId;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        public Builder sp(@Nullable Long sp) {
            this.sp = sp;
            return this;
        }

        /**
         * Defensively copied on {@link #build()}.
         */
        public Builder activities(@Nullable List<Activity> activities) {
            this.activities = activities;
            return this;
        }

        public Builder curInventorySlots(@Nullable Integer curInventorySlots) {
            this.curInventorySlots = curInventorySlots;
            return this;
        }

        public Builder maxInventorySlots(@Nullable Integer maxInventorySlots) {
            this.maxInventorySlots = maxInventorySlots;
            return this;
        }

        public Builder curQuestInventorySlots(@Nullable Integer curQuestInventorySlots) {
            this.curQuestInventorySlots = curQuestInventorySlots;
            return this;
        }

        public Builder maxQuestInventorySlots(@Nullable Integer maxQuestInventorySlots) {
            this.maxQuestInventorySlots = maxQuestInventorySlots;
            return this;
        }

        public Builder curWeight(@Nullable Integer curWeight) {
            this.curWeight = curWeight;
            return this;
        }

        public Builder maxWeight(@Nullable Integer maxWeight) {
            this.maxWeight = maxWeight;
            return this;
        }

        public CharacterRuntimeDto build() {
            return new CharacterRuntimeDto(
                    id,
                    curHp,
                    maxHp,
                    curMp,
                    maxMp,
                    curCp,
                    maxCp,
                    curVit,
                    maxVit,
                    x,
                    y,
                    z,
                    online,
                    aiStatus,
                    classId,
                    level,
                    exp,
                    sp,
                    activities,
                    curInventorySlots,
                    maxInventorySlots,
                    curQuestInventorySlots,
                    maxQuestInventorySlots,
                    curWeight,
                    maxWeight);
        }
    }
}
