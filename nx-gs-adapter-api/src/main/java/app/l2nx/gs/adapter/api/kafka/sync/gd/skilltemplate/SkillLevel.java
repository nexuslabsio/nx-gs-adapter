package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One base level of a {@link SkillTemplate} — the per-level stats, localization and effects.
 * In L2 a skill scales across levels (mp cost, power, cool/reuse times grow); each
 * level is a distinct row carried in {@link SkillTemplate#getLevels()}.
 *
 * <p>{@code level} is the non-null identity within the skill. Every other field is
 * {@link Nullable}. Fields with a unit carry it in the name: time fields are
 * {@code *Ms} (milliseconds, {@code hitTimeMs}/{@code coolTimeMs}/{@code reuseDelayMs})
 * or {@code *Sec} ({@code abnormalTimeSec}); consumption / range / level
 * fields are unit-bare {@code Integer}. {@code name} / {@code description} are
 * locale-keyed {@link LocalizedText}. {@code effects} is the per-level effect list.</p>
 *
 * <p>Enchant variants (route-enchanted levels) are NOT here — they ride
 * {@link SkillTemplate#getEnchantRoutes()} as {@link SkillEnchantRoute}. This list is the
 * base ladder only.</p>
 */
public final class SkillLevel {

    private final int level;
    private final @Nullable String icon;
    private final @Nullable Integer mpConsume;
    private final @Nullable Integer mpInitialConsume;
    private final @Nullable Integer hpConsume;
    private final @Nullable Integer itemTemplateId;
    private final @Nullable Integer itemTemplateCount;
    private final @Nullable Integer castRange;
    private final @Nullable Integer effectRange;
    private final @Nullable Integer magicLevel;
    private final @Nullable Integer abnormalLvl;
    private final @Nullable Integer abnormalTimeSec;
    private final @Nullable Integer hitTimeMs;
    private final @Nullable Integer coolTimeMs;
    private final @Nullable Integer reuseDelayMs;
    private final @Nullable Double power;
    private final @Nullable LocalizedText name;
    private final @Nullable LocalizedText description;
    private final @Nullable List<SkillEffect> effects;

    private SkillLevel(Builder b) {
        this.level = b.level;
        this.icon = b.icon;
        this.mpConsume = b.mpConsume;
        this.mpInitialConsume = b.mpInitialConsume;
        this.hpConsume = b.hpConsume;
        this.itemTemplateId = b.itemTemplateId;
        this.itemTemplateCount = b.itemTemplateCount;
        this.castRange = b.castRange;
        this.effectRange = b.effectRange;
        this.magicLevel = b.magicLevel;
        this.abnormalLvl = b.abnormalLvl;
        this.abnormalTimeSec = b.abnormalTimeSec;
        this.hitTimeMs = b.hitTimeMs;
        this.coolTimeMs = b.coolTimeMs;
        this.reuseDelayMs = b.reuseDelayMs;
        this.power = b.power;
        this.name = b.name;
        this.description = b.description;
        this.effects = b.effects == null ? null
                : Collections.unmodifiableList(new ArrayList<SkillEffect>(b.effects));
    }

    public int getLevel() {
        return level;
    }

    public @Nullable String getIcon() {
        return icon;
    }

    public @Nullable Integer getMpConsume() {
        return mpConsume;
    }

    /**
     * MP paid up-front when the cast starts (vs the total {@code mpConsume}).
     */
    public @Nullable Integer getMpInitialConsume() {
        return mpInitialConsume;
    }

    public @Nullable Integer getHpConsume() {
        return hpConsume;
    }

    public @Nullable Integer getItemConsumeId() {
        return itemTemplateId;
    }

    public @Nullable Integer getItemConsumeCount() {
        return itemTemplateCount;
    }

    public @Nullable Integer getCastRange() {
        return castRange;
    }

    public @Nullable Integer getEffectRange() {
        return effectRange;
    }

    public @Nullable Integer getMagicLevel() {
        return magicLevel;
    }

    /**
     * Abnormal (buff/debuff) slot level — higher overwrites lower of the same type.
     */
    public @Nullable Integer getAbnormalLvl() {
        return abnormalLvl;
    }

    /**
     * Abnormal (buff/debuff) duration.
     */
    public @Nullable Integer getAbnormalTimeSec() {
        return abnormalTimeSec;
    }

    /**
     * Cast animation time.
     */
    public @Nullable Integer getHitTimeMs() {
        return hitTimeMs;
    }

    /**
     * Cool time (post-cast recovery).
     */
    public @Nullable Integer getCoolTimeMs() {
        return coolTimeMs;
    }

    /**
     * Cooldown before reuse.
     */
    public @Nullable Integer getReuseDelayMs() {
        return reuseDelayMs;
    }

    /**
     * SkillTemplate power (damage / heal magnitude) — a coefficient, no unit.
     */
    public @Nullable Double getPower() {
        return power;
    }

    public @Nullable LocalizedText getName() {
        return name;
    }

    public @Nullable LocalizedText getDescription() {
        return description;
    }

    /**
     * Effects applied at this level; {@code null} if none.
     */
    public @Nullable List<SkillEffect> getEffects() {
        return effects;
    }

    public Builder toBuilder() {
        return new Builder()
                .level(level)
                .icon(icon)
                .mpConsume(mpConsume)
                .mpInitialConsume(mpInitialConsume)
                .hpConsume(hpConsume)
                .itemTemplateId(itemTemplateId)
                .itemTemplateCount(itemTemplateCount)
                .castRange(castRange)
                .effectRange(effectRange)
                .magicLevel(magicLevel)
                .abnormalLvl(abnormalLvl)
                .abnormalTimeSec(abnormalTimeSec)
                .hitTimeMs(hitTimeMs)
                .coolTimeMs(coolTimeMs)
                .reuseDelayMs(reuseDelayMs)
                .power(power)
                .name(name)
                .description(description)
                .effects(effects);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillLevel)) return false;
        SkillLevel that = (SkillLevel) o;
        return level == that.level
                && Objects.equals(icon, that.icon)
                && Objects.equals(mpConsume, that.mpConsume)
                && Objects.equals(mpInitialConsume, that.mpInitialConsume)
                && Objects.equals(hpConsume, that.hpConsume)
                && Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(itemTemplateCount, that.itemTemplateCount)
                && Objects.equals(castRange, that.castRange)
                && Objects.equals(effectRange, that.effectRange)
                && Objects.equals(magicLevel, that.magicLevel)
                && Objects.equals(abnormalLvl, that.abnormalLvl)
                && Objects.equals(abnormalTimeSec, that.abnormalTimeSec)
                && Objects.equals(hitTimeMs, that.hitTimeMs)
                && Objects.equals(coolTimeMs, that.coolTimeMs)
                && Objects.equals(reuseDelayMs, that.reuseDelayMs)
                && Objects.equals(power, that.power)
                && Objects.equals(name, that.name)
                && Objects.equals(description, that.description)
                && Objects.equals(effects, that.effects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, icon, mpConsume, mpInitialConsume, hpConsume, itemTemplateId,
                itemTemplateCount, castRange, effectRange, magicLevel, abnormalLvl, abnormalTimeSec,
                hitTimeMs, coolTimeMs, reuseDelayMs, power, name, description, effects);
    }

    @Override
    public String toString() {
        return "SkillLevel[level=" + level + ", power=" + power + ", mpConsume=" + mpConsume + "]";
    }

    public static final class Builder {
        private int level;
        private @Nullable String icon;
        private @Nullable Integer mpConsume;
        private @Nullable Integer mpInitialConsume;
        private @Nullable Integer hpConsume;
        private @Nullable Integer itemTemplateId;
        private @Nullable Integer itemTemplateCount;
        private @Nullable Integer castRange;
        private @Nullable Integer effectRange;
        private @Nullable Integer magicLevel;
        private @Nullable Integer abnormalLvl;
        private @Nullable Integer abnormalTimeSec;
        private @Nullable Integer hitTimeMs;
        private @Nullable Integer coolTimeMs;
        private @Nullable Integer reuseDelayMs;
        private @Nullable Double power;
        private @Nullable LocalizedText name;
        private @Nullable LocalizedText description;
        private @Nullable List<SkillEffect> effects;

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public Builder icon(@Nullable String icon) {
            this.icon = icon;
            return this;
        }

        public Builder mpConsume(@Nullable Integer mpConsume) {
            this.mpConsume = mpConsume;
            return this;
        }

        public Builder mpInitialConsume(@Nullable Integer mpInitialConsume) {
            this.mpInitialConsume = mpInitialConsume;
            return this;
        }

        public Builder hpConsume(@Nullable Integer hpConsume) {
            this.hpConsume = hpConsume;
            return this;
        }

        public Builder itemTemplateId(@Nullable Integer itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder itemTemplateCount(@Nullable Integer itemTemplateCount) {
            this.itemTemplateCount = itemTemplateCount;
            return this;
        }

        public Builder castRange(@Nullable Integer castRange) {
            this.castRange = castRange;
            return this;
        }

        public Builder effectRange(@Nullable Integer effectRange) {
            this.effectRange = effectRange;
            return this;
        }

        public Builder magicLevel(@Nullable Integer magicLevel) {
            this.magicLevel = magicLevel;
            return this;
        }

        public Builder abnormalLvl(@Nullable Integer abnormalLvl) {
            this.abnormalLvl = abnormalLvl;
            return this;
        }

        public Builder abnormalTimeSec(@Nullable Integer abnormalTimeSec) {
            this.abnormalTimeSec = abnormalTimeSec;
            return this;
        }

        public Builder hitTimeMs(@Nullable Integer hitTimeMs) {
            this.hitTimeMs = hitTimeMs;
            return this;
        }

        public Builder coolTimeMs(@Nullable Integer coolTimeMs) {
            this.coolTimeMs = coolTimeMs;
            return this;
        }

        public Builder reuseDelayMs(@Nullable Integer reuseDelayMs) {
            this.reuseDelayMs = reuseDelayMs;
            return this;
        }

        public Builder power(@Nullable Double power) {
            this.power = power;
            return this;
        }

        public Builder name(@Nullable LocalizedText name) {
            this.name = name;
            return this;
        }

        public Builder description(@Nullable LocalizedText description) {
            this.description = description;
            return this;
        }

        public Builder effects(@Nullable List<SkillEffect> effects) {
            this.effects = effects;
            return this;
        }

        public SkillLevel build() {
            return new SkillLevel(this);
        }
    }
}
