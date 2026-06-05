package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One enchant-route variant of a {@link SkillTemplate} — an enchanted level beyond the base
 * ladder. In L2 a max-level skill can be enchanted along one of several routes
 * (power / cost / range / time tracks), each with its own enchant levels
 * ({@code +1..+N}); the core models these as the same skill object with a non-zero
 * sub-level, so a route variant carries the same per-level stats as a base
 * {@link SkillLevel} plus the route coordinates and enchant localization.
 *
 * <p>{@code baseLevel} (the skill level being enchanted), {@code route}, and
 * {@code enchantLevel} are the non-null identity. Stats mirror {@link SkillLevel}
 * (the variant is a full skill object), units carried in the name ({@code *Ms} /
 * {@code *Sec}). {@code enchantName} / {@code enchantDescription} are the
 * enchanted-variant localization. Per-route effects are out of scope — the enchant
 * primarily shifts the numbers carried here.</p>
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
    private final @Nullable Integer abnormalLvl;
    private final @Nullable Integer abnormalTimeSec;
    private final @Nullable Integer hitTimeMs;
    private final @Nullable Integer coolTimeMs;
    private final @Nullable Integer reuseDelayMs;
    private final @Nullable Double power;
    private final @Nullable String enchantIcon;
    private final @Nullable LocalizedText enchantName;
    private final @Nullable LocalizedText enchantDescription;
    private final @Nullable Long enchantAdena;
    private final @Nullable Long enchantSp;

    public SkillEnchantRoute(int baseLevel,
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
                             @Nullable Integer abnormalLvl,
                             @Nullable Integer abnormalTimeSec,
                             @Nullable Integer hitTimeMs,
                             @Nullable Integer coolTimeMs,
                             @Nullable Integer reuseDelayMs,
                             @Nullable Double power,
                             @Nullable String enchantIcon,
                             @Nullable LocalizedText enchantName,
                             @Nullable LocalizedText enchantDescription,
                             @Nullable Long enchantAdena,
                             @Nullable Long enchantSp) {
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
        this.abnormalLvl = abnormalLvl;
        this.abnormalTimeSec = abnormalTimeSec;
        this.hitTimeMs = hitTimeMs;
        this.coolTimeMs = coolTimeMs;
        this.reuseDelayMs = reuseDelayMs;
        this.power = power;
        this.enchantIcon = enchantIcon;
        this.enchantName = enchantName;
        this.enchantDescription = enchantDescription;
        this.enchantAdena = enchantAdena;
        this.enchantSp = enchantSp;
    }

    /**
     * The base skill level this enchant route applies to.
     */
    public int getBaseLevel() {
        return baseLevel;
    }

    /**
     * Enchant route number (which track — power / cost / time / …).
     */
    public int getRoute() {
        return route;
    }

    /**
     * Enchant level within the route ({@code +1..+N}).
     */
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
     * Cool time.
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

    public @Nullable Double getPower() {
        return power;
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

    /**
     * Adena cost to apply this enchant step; {@code null} if the build does not expose it.
     */
    public @Nullable Long getEnchantAdena() {
        return enchantAdena;
    }

    /**
     * SP cost to apply this enchant step; {@code null} if the build does not expose it.
     */
    public @Nullable Long getEnchantSp() {
        return enchantSp;
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
                .abnormalLvl(abnormalLvl)
                .abnormalTimeSec(abnormalTimeSec)
                .hitTimeMs(hitTimeMs)
                .coolTimeMs(coolTimeMs)
                .reuseDelayMs(reuseDelayMs)
                .power(power)
                .enchantIcon(enchantIcon)
                .enchantName(enchantName)
                .enchantDescription(enchantDescription)
                .enchantAdena(enchantAdena)
                .enchantSp(enchantSp);
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
                && Objects.equals(abnormalLvl, that.abnormalLvl)
                && Objects.equals(abnormalTimeSec, that.abnormalTimeSec)
                && Objects.equals(hitTimeMs, that.hitTimeMs)
                && Objects.equals(coolTimeMs, that.coolTimeMs)
                && Objects.equals(reuseDelayMs, that.reuseDelayMs)
                && Objects.equals(power, that.power)
                && Objects.equals(enchantIcon, that.enchantIcon)
                && Objects.equals(enchantName, that.enchantName)
                && Objects.equals(enchantDescription, that.enchantDescription)
                && Objects.equals(enchantAdena, that.enchantAdena)
                && Objects.equals(enchantSp, that.enchantSp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseLevel, route, enchantLevel, mpConsume, mpInitialConsume, hpConsume,
                itemTemplateId, itemTemplateCount, castRange, effectRange, magicLevel, abnormalLvl,
                abnormalTimeSec, hitTimeMs, coolTimeMs, reuseDelayMs, power, enchantIcon,
                enchantName, enchantDescription, enchantAdena, enchantSp);
    }

    @Override
    public String toString() {
        return "SkillEnchantRoute[baseLevel=" + baseLevel + ", route=" + route
                + ", enchantLevel=" + enchantLevel + "]";
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
        private @Nullable Integer abnormalLvl;
        private @Nullable Integer abnormalTimeSec;
        private @Nullable Integer hitTimeMs;
        private @Nullable Integer coolTimeMs;
        private @Nullable Integer reuseDelayMs;
        private @Nullable Double power;
        private @Nullable String enchantIcon;
        private @Nullable LocalizedText enchantName;
        private @Nullable LocalizedText enchantDescription;
        private @Nullable Long enchantAdena;
        private @Nullable Long enchantSp;

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

        public Builder enchantSp(@Nullable Long enchantSp) {
            this.enchantSp = enchantSp;
            return this;
        }

        public SkillEnchantRoute build() {
            return new SkillEnchantRoute(baseLevel, route, enchantLevel, mpConsume,
                    mpInitialConsume, hpConsume, itemTemplateId, itemTemplateCount, castRange,
                    effectRange, magicLevel, abnormalLvl, abnormalTimeSec, hitTimeMs, coolTimeMs,
                    reuseDelayMs, power, enchantIcon, enchantName, enchantDescription, enchantAdena,
                    enchantSp);
        }
    }
}
