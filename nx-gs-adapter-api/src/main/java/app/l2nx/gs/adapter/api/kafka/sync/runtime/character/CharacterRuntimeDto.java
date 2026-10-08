package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.Activity;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.CharacterEffect;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.WellKnownActivities;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.WellKnownActivityMetadata;
import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.WellKnownAiStatuses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Volatile runtime state of one character; only {@code id} is required. {@code online} null/omitted means ONLINE;
 * explicit {@code false} is a one-shot tombstone with everything else null.
 *
 * <p>An offline trader is not a tombstone: it ticks with {@code online=false} and an
 * {@link WellKnownActivities#OFFLINE_TRADE} activity.</p>
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
    private final @Nullable List<CharacterEffect> effects;

    /**
     * Must stay the only constructor: an overload makes creator detection ambiguous and consumers stop deserializing
     * the whole channel. Grow the wire by appending parameters.
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
            @Nullable Integer maxWeight,
            @Nullable List<CharacterEffect> effects) {
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
        this.effects = copy(effects);
    }

    private static <T> @Nullable List<T> copy(@Nullable List<T> values) {
        return values == null ? null : Collections.unmodifiableList(new ArrayList<T>(values));
    }

    public long getId() {
        return id;
    }

    /**
     * False only for the tombstone; offline traders still carry state. Enumerates every volatile field by hand: a
     * missed one turns an ordinary tick into a tombstone for every consumer.
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
                || maxWeight != null
                || effects != null;
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

    public @Nullable Boolean getOnline() {
        return online;
    }

    /** Open string; canonical values in {@link WellKnownAiStatuses}. */
    public @Nullable String getAiStatus() {
        return aiStatus;
    }

    /**
     * Class that {@link #getLevel()}, {@link #getExp()} and {@link #getSp()} describe, not necessarily the main class;
     * {@code exp} is an absolute total. Null when the source ID is outside {@link CharacterClass}.
     */
    public @Nullable CharacterClass getClassId() {
        return classId;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    public @Nullable Long getSp() {
        return sp;
    }

    public @Nullable Long getExp() {
        return exp;
    }

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

    public @Nullable Integer getCurWeight() {
        return curWeight;
    }

    public @Nullable Integer getMaxWeight() {
        return maxWeight;
    }

    /** Independent of {@link #getAiStatus() aiStatus}; metadata keys in {@link WellKnownActivityMetadata}. */
    public @Nullable List<Activity> getActivities() {
        return activities;
    }

    /**
     * Status-bar effects in bar order. Null on a state-carrying row means the host does not report effects, which
     * leaves the consumer with none; an empty list also means none.
     */
    public @Nullable List<CharacterEffect> getEffects() {
        return effects;
    }

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
                .maxWeight(maxWeight)
                .effects(effects);
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
                && Objects.equals(maxWeight, that.maxWeight)
                && Objects.equals(effects, that.effects);
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
                maxWeight,
                effects);
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
                + ", curWeight=" + curWeight + ", maxWeight=" + maxWeight
                + ", effects=" + effects + "]";
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
        private @Nullable List<CharacterEffect> effects;

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

        public Builder effects(@Nullable List<CharacterEffect> effects) {
            this.effects = effects;
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
                    maxWeight,
                    effects);
        }
    }
}
