package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One enchant-route variant of a {@link Skill}: an enchanted level beyond the base ladder, carrying the same
 * per-level stats as {@link SkillLevel} plus route coordinates. {@code baseLevel}, {@code route} and
 * {@code enchantLevel} are the non-null identity; providers MUST emit the skill's max base level as
 * {@code baseLevel}, since enchanting is only available once the base ladder is maxed.
 */
public final class SkillEnchantRoute {

    private final int baseLevel;
    private final int route;
    private final int enchantLevel;
    private final @Nullable Integer mpConsume;
    private final @Nullable Integer mpInitialConsume;
    private final @Nullable Integer hpConsume;
    private final @Nullable Integer itemTemplateId;
    private final @Nullable Integer itemTemplateCount;
    private final @Nullable Integer castRange;
    private final @Nullable Integer effectRange;
    private final @Nullable Integer magicLevel;
    private final @Nullable Integer abnormalLevel;
    private final @Nullable Integer abnormalTimeSec;
    private final @Nullable Integer hitTimeMs;
    private final @Nullable Integer coolTimeMs;
    private final @Nullable Integer reuseDelayMs;
    private final @Nullable Double power;
    private final @Nullable String attribute;
    private final @Nullable Integer attributePower;
    private final @Nullable String enchantIcon;
    private final @Nullable LocalizedText enchantName;
    private final @Nullable LocalizedText enchantDescription;
    private final @Nullable Long enchantAdena;
    private final @Nullable Long enchantExp;
    private final @Nullable Long enchantSp;
    private final @Nullable Map<Integer, Integer> enchantChanceByCharLevelPercent;

    public SkillEnchantRoute(
            int baseLevel,
            int route,
            int enchantLevel,
            @Nullable Integer mpConsume,
            @Nullable Integer mpInitialConsume,
            @Nullable Integer hpConsume,
            @Nullable Integer itemTemplateId,
            @Nullable Integer itemTemplateCount,
            @Nullable Integer castRange,
            @Nullable Integer effectRange,
            @Nullable Integer magicLevel,
            @Nullable Integer abnormalLevel,
            @Nullable Integer abnormalTimeSec,
            @Nullable Integer hitTimeMs,
            @Nullable Integer coolTimeMs,
            @Nullable Integer reuseDelayMs,
            @Nullable Double power,
            @Nullable String attribute,
            @Nullable Integer attributePower,
            @Nullable String enchantIcon,
            @Nullable LocalizedText enchantName,
            @Nullable LocalizedText enchantDescription,
            @Nullable Long enchantAdena,
            @Nullable Long enchantExp,
            @Nullable Long enchantSp,
            @Nullable Map<Integer, Integer> enchantChanceByCharLevelPercent) {
        this.baseLevel = baseLevel;
        this.route = route;
        this.enchantLevel = enchantLevel;
        this.mpConsume = mpConsume;
        this.mpInitialConsume = mpInitialConsume;
        this.hpConsume = hpConsume;
        this.itemTemplateId = itemTemplateId;
        this.itemTemplateCount = itemTemplateCount;
        this.castRange = castRange;
        this.effectRange = effectRange;
        this.magicLevel = magicLevel;
        this.abnormalLevel = abnormalLevel;
        this.abnormalTimeSec = abnormalTimeSec;
        this.hitTimeMs = hitTimeMs;
        this.coolTimeMs = coolTimeMs;
        this.reuseDelayMs = reuseDelayMs;
        this.power = power;
        this.attribute = attribute;
        this.attributePower = attributePower;
        this.enchantIcon = enchantIcon;
        this.enchantName = enchantName;
        this.enchantDescription = enchantDescription;
        this.enchantAdena = enchantAdena;
        this.enchantExp = enchantExp;
        this.enchantSp = enchantSp;
        this.enchantChanceByCharLevelPercent = enchantChanceByCharLevelPercent == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<Integer, Integer>(enchantChanceByCharLevelPercent));
    }

    /** Always the skill's max base level. */
    public int getBaseLevel() {
        return baseLevel;
    }

    public int getRoute() {
        return route;
    }

    public int getEnchantLevel() {
        return enchantLevel;
    }

    public @Nullable Integer getMpConsume() {
        return mpConsume;
    }

    public @Nullable Integer getMpInitialConsume() {
        return mpInitialConsume;
    }

    public @Nullable Integer getHpConsume() {
        return hpConsume;
    }

    public @Nullable Integer getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Integer getItemTemplateCount() {
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

    public @Nullable Integer getAbnormalLevel() {
        return abnormalLevel;
    }

    public @Nullable Integer getAbnormalTimeSec() {
        return abnormalTimeSec;
    }

    public @Nullable Integer getHitTimeMs() {
        return hitTimeMs;
    }

    public @Nullable Integer getCoolTimeMs() {
        return coolTimeMs;
    }

    public @Nullable Integer getReuseDelayMs() {
        return reuseDelayMs;
    }

    public @Nullable Double getPower() {
        return power;
    }

    /** Element added by the route, e.g. a "+3 Fire Attack" route; {@code null} when it adds none. */
    public @Nullable String getAttribute() {
        return attribute;
    }

    public @Nullable Integer getAttributePower() {
        return attributePower;
    }

    public @Nullable String getEnchantIcon() {
        return enchantIcon;
    }

    public @Nullable LocalizedText getEnchantName() {
        return enchantName;
    }

    public @Nullable LocalizedText getEnchantDescription() {
        return enchantDescription;
    }

    public @Nullable Long getEnchantAdena() {
        return enchantAdena;
    }

    public @Nullable Long getEnchantExp() {
        return enchantExp;
    }

    public @Nullable Long getEnchantSp() {
        return enchantSp;
    }

    /** Keyed by the enchanting character's level; levels without a defined chance are absent. */
    public @Nullable Map<Integer, Integer> getEnchantChanceByCharLevelPercent() {
        return enchantChanceByCharLevelPercent;
    }

    public Builder toBuilder() {
        return new Builder()
                .baseLevel(baseLevel)
                .route(route)
                .enchantLevel(enchantLevel)
                .mpConsume(mpConsume)
                .mpInitialConsume(mpInitialConsume)
                .hpConsume(hpConsume)
                .itemTemplateId(itemTemplateId)
                .itemTemplateCount(itemTemplateCount)
                .castRange(castRange)
                .effectRange(effectRange)
                .magicLevel(magicLevel)
                .abnormalLevel(abnormalLevel)
                .abnormalTimeSec(abnormalTimeSec)
                .hitTimeMs(hitTimeMs)
                .coolTimeMs(coolTimeMs)
                .reuseDelayMs(reuseDelayMs)
                .power(power)
                .attribute(attribute)
                .attributePower(attributePower)
                .enchantIcon(enchantIcon)
                .enchantName(enchantName)
                .enchantDescription(enchantDescription)
                .enchantAdena(enchantAdena)
                .enchantExp(enchantExp)
                .enchantSp(enchantSp)
                .enchantChanceByCharLevelPercent(enchantChanceByCharLevelPercent);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillEnchantRoute)) return false;
        SkillEnchantRoute that = (SkillEnchantRoute) o;
        return baseLevel == that.baseLevel
                && route == that.route
                && enchantLevel == that.enchantLevel
                && Objects.equals(mpConsume, that.mpConsume)
                && Objects.equals(mpInitialConsume, that.mpInitialConsume)
                && Objects.equals(hpConsume, that.hpConsume)
                && Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(itemTemplateCount, that.itemTemplateCount)
                && Objects.equals(castRange, that.castRange)
                && Objects.equals(effectRange, that.effectRange)
                && Objects.equals(magicLevel, that.magicLevel)
                && Objects.equals(abnormalLevel, that.abnormalLevel)
                && Objects.equals(abnormalTimeSec, that.abnormalTimeSec)
                && Objects.equals(hitTimeMs, that.hitTimeMs)
                && Objects.equals(coolTimeMs, that.coolTimeMs)
                && Objects.equals(reuseDelayMs, that.reuseDelayMs)
                && Objects.equals(power, that.power)
                && Objects.equals(attribute, that.attribute)
                && Objects.equals(attributePower, that.attributePower)
                && Objects.equals(enchantIcon, that.enchantIcon)
                && Objects.equals(enchantName, that.enchantName)
                && Objects.equals(enchantDescription, that.enchantDescription)
                && Objects.equals(enchantAdena, that.enchantAdena)
                && Objects.equals(enchantExp, that.enchantExp)
                && Objects.equals(enchantSp, that.enchantSp)
                && Objects.equals(enchantChanceByCharLevelPercent, that.enchantChanceByCharLevelPercent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                baseLevel,
                route,
                enchantLevel,
                mpConsume,
                mpInitialConsume,
                hpConsume,
                itemTemplateId,
                itemTemplateCount,
                castRange,
                effectRange,
                magicLevel,
                abnormalLevel,
                abnormalTimeSec,
                hitTimeMs,
                coolTimeMs,
                reuseDelayMs,
                power,
                attribute,
                attributePower,
                enchantIcon,
                enchantName,
                enchantDescription,
                enchantAdena,
                enchantExp,
                enchantSp,
                enchantChanceByCharLevelPercent);
    }

    @Override
    public String toString() {
        return "SkillEnchantRoute[baseLevel=" + baseLevel + ", route=" + route + ", enchantLevel=" + enchantLevel + "]";
    }

    public static final class Builder {
        private int baseLevel;
        private int route;
        private int enchantLevel;
        private @Nullable Integer mpConsume;
        private @Nullable Integer mpInitialConsume;
        private @Nullable Integer hpConsume;
        private @Nullable Integer itemTemplateId;
        private @Nullable Integer itemTemplateCount;
        private @Nullable Integer castRange;
        private @Nullable Integer effectRange;
        private @Nullable Integer magicLevel;
        private @Nullable Integer abnormalLevel;
        private @Nullable Integer abnormalTimeSec;
        private @Nullable Integer hitTimeMs;
        private @Nullable Integer coolTimeMs;
        private @Nullable Integer reuseDelayMs;
        private @Nullable Double power;
        private @Nullable String attribute;
        private @Nullable Integer attributePower;
        private @Nullable String enchantIcon;
        private @Nullable LocalizedText enchantName;
        private @Nullable LocalizedText enchantDescription;
        private @Nullable Long enchantAdena;
        private @Nullable Long enchantExp;
        private @Nullable Long enchantSp;
        private @Nullable Map<Integer, Integer> enchantChanceByCharLevelPercent;

        public Builder baseLevel(int baseLevel) {
            this.baseLevel = baseLevel;
            return this;
        }

        public Builder route(int route) {
            this.route = route;
            return this;
        }

        public Builder enchantLevel(int enchantLevel) {
            this.enchantLevel = enchantLevel;
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

        public Builder abnormalLevel(@Nullable Integer abnormalLevel) {
            this.abnormalLevel = abnormalLevel;
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

        public Builder attribute(@Nullable String attribute) {
            this.attribute = attribute;
            return this;
        }

        public Builder attributePower(@Nullable Integer attributePower) {
            this.attributePower = attributePower;
            return this;
        }

        public Builder enchantIcon(@Nullable String enchantIcon) {
            this.enchantIcon = enchantIcon;
            return this;
        }

        public Builder enchantName(@Nullable LocalizedText enchantName) {
            this.enchantName = enchantName;
            return this;
        }

        public Builder enchantDescription(@Nullable LocalizedText enchantDescription) {
            this.enchantDescription = enchantDescription;
            return this;
        }

        public Builder enchantAdena(@Nullable Long enchantAdena) {
            this.enchantAdena = enchantAdena;
            return this;
        }

        public Builder enchantExp(@Nullable Long enchantExp) {
            this.enchantExp = enchantExp;
            return this;
        }

        public Builder enchantSp(@Nullable Long enchantSp) {
            this.enchantSp = enchantSp;
            return this;
        }

        public Builder enchantChanceByCharLevelPercent(
                @Nullable Map<Integer, Integer> enchantChanceByCharLevelPercent) {
            this.enchantChanceByCharLevelPercent = enchantChanceByCharLevelPercent;
            return this;
        }

        public SkillEnchantRoute build() {
            return new SkillEnchantRoute(
                    baseLevel,
                    route,
                    enchantLevel,
                    mpConsume,
                    mpInitialConsume,
                    hpConsume,
                    itemTemplateId,
                    itemTemplateCount,
                    castRange,
                    effectRange,
                    magicLevel,
                    abnormalLevel,
                    abnormalTimeSec,
                    hitTimeMs,
                    coolTimeMs,
                    reuseDelayMs,
                    power,
                    attribute,
                    attributePower,
                    enchantIcon,
                    enchantName,
                    enchantDescription,
                    enchantAdena,
                    enchantExp,
                    enchantSp,
                    enchantChanceByCharLevelPercent);
        }
    }
}
