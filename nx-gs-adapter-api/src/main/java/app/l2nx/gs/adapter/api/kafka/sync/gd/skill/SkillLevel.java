package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One base level of a {@link Skill} — the per-level stats, localization and effects.
 * In L2 a skill scales across levels (mp cost, power, cool/reuse times grow); each
 * level is a distinct row carried in {@link Skill#getLevels()}.
 *
 * <p>{@code level} is the non-null identity within the skill. Every other field is
 * {@link Nullable}. Fields with a unit carry it in the name: time fields are
 * {@code *Ms} (milliseconds, {@code hitTimeMs}/{@code coolTimeMs}/{@code reuseDelayMs})
 * or {@code *Sec} ({@code abnormalTimeSec}); chance fields are {@code *Percent};
 * consumption / range / level fields are unit-bare {@code Integer}. {@code name} /
 * {@code description} are locale-keyed {@link LocalizedText}. {@code effects} is the
 * per-level effect list (target / self / passive channels, see
 * {@link SkillEffect#getKind()}); {@code statModifiers} are stat modifications attached
 * directly to the skill (no effect wrapper — typical for simple passives).</p>
 *
 * <p>{@code attribute} is the offensive element this level carries
 * ({@code FIRE}/{@code WATER}/{@code WIND}/{@code EARTH}/{@code HOLY}/{@code DARK});
 * {@code attributePower} is the element value. Both {@code null} when the level has no
 * offensive attribute.</p>
 *
 * <p>Enchant variants (route-enchanted levels) are NOT here — they ride
 * {@link Skill#getEnchantRoutes()} as {@link SkillEnchantRoute}. This list is the
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
    private final @Nullable Integer soulMaxConsume;
    private final @Nullable Integer energyConsume;
    private final @Nullable Integer chargeConsume;
    private final @Nullable Integer castRange;
    private final @Nullable Integer effectRange;
    private final @Nullable Integer affectRange;
    private final @Nullable Integer affectLimit;
    private final @Nullable Integer fanStartAngle;
    private final @Nullable Integer fanRadius;
    private final @Nullable Integer fanAngle;
    private final @Nullable Integer magicLevel;
    private final @Nullable Integer abnormalLevel;
    private final @Nullable Integer abnormalTimeSec;
    private final @Nullable Integer hitTimeMs;
    private final @Nullable Integer coolTimeMs;
    private final @Nullable Integer reuseDelayMs;
    private final @Nullable Integer baseCritRate;
    private final @Nullable Double power;
    private final @Nullable Double pvpPower;
    private final @Nullable Double pvePower;
    private final @Nullable Integer minChancePercent;
    private final @Nullable Integer maxChancePercent;
    private final @Nullable Integer activateRatePercent;
    private final @Nullable Integer levelModifier;
    private final @Nullable Integer lethalStrikeRatePercent;
    private final @Nullable Integer halfKillRatePercent;
    private final @Nullable Integer negateRatePercent;
    private final @Nullable Map<String, Integer> negateAbnormalTypes;
    private final @Nullable Integer aggroPoints;
    private final @Nullable String attribute;
    private final @Nullable Integer attributePower;
    private final @Nullable LocalizedText name;
    private final @Nullable LocalizedText description;
    private final @Nullable List<SkillEffect> effects;
    private final @Nullable List<SkillStatModifier> statModifiers;
    private final @Nullable List<SkillProducedItemGroup> producedItems;

    public SkillLevel(
            int level,
            @Nullable String icon,
            @Nullable Integer mpConsume,
            @Nullable Integer mpInitialConsume,
            @Nullable Integer hpConsume,
            @Nullable Integer itemTemplateId,
            @Nullable Integer itemTemplateCount,
            @Nullable Integer soulMaxConsume,
            @Nullable Integer energyConsume,
            @Nullable Integer chargeConsume,
            @Nullable Integer castRange,
            @Nullable Integer effectRange,
            @Nullable Integer affectRange,
            @Nullable Integer affectLimit,
            @Nullable Integer fanStartAngle,
            @Nullable Integer fanRadius,
            @Nullable Integer fanAngle,
            @Nullable Integer magicLevel,
            @Nullable Integer abnormalLevel,
            @Nullable Integer abnormalTimeSec,
            @Nullable Integer hitTimeMs,
            @Nullable Integer coolTimeMs,
            @Nullable Integer reuseDelayMs,
            @Nullable Integer baseCritRate,
            @Nullable Double power,
            @Nullable Double pvpPower,
            @Nullable Double pvePower,
            @Nullable Integer minChancePercent,
            @Nullable Integer maxChancePercent,
            @Nullable Integer activateRatePercent,
            @Nullable Integer levelModifier,
            @Nullable Integer lethalStrikeRatePercent,
            @Nullable Integer halfKillRatePercent,
            @Nullable Integer negateRatePercent,
            @Nullable Map<String, Integer> negateAbnormalTypes,
            @Nullable Integer aggroPoints,
            @Nullable String attribute,
            @Nullable Integer attributePower,
            @Nullable LocalizedText name,
            @Nullable LocalizedText description,
            @Nullable List<SkillEffect> effects,
            @Nullable List<SkillStatModifier> statModifiers,
            @Nullable List<SkillProducedItemGroup> producedItems) {
        this.level = level;
        this.icon = icon;
        this.mpConsume = mpConsume;
        this.mpInitialConsume = mpInitialConsume;
        this.hpConsume = hpConsume;
        this.itemTemplateId = itemTemplateId;
        this.itemTemplateCount = itemTemplateCount;
        this.soulMaxConsume = soulMaxConsume;
        this.energyConsume = energyConsume;
        this.chargeConsume = chargeConsume;
        this.castRange = castRange;
        this.effectRange = effectRange;
        this.affectRange = affectRange;
        this.affectLimit = affectLimit;
        this.fanStartAngle = fanStartAngle;
        this.fanRadius = fanRadius;
        this.fanAngle = fanAngle;
        this.magicLevel = magicLevel;
        this.abnormalLevel = abnormalLevel;
        this.abnormalTimeSec = abnormalTimeSec;
        this.hitTimeMs = hitTimeMs;
        this.coolTimeMs = coolTimeMs;
        this.reuseDelayMs = reuseDelayMs;
        this.baseCritRate = baseCritRate;
        this.power = power;
        this.pvpPower = pvpPower;
        this.pvePower = pvePower;
        this.minChancePercent = minChancePercent;
        this.maxChancePercent = maxChancePercent;
        this.activateRatePercent = activateRatePercent;
        this.levelModifier = levelModifier;
        this.lethalStrikeRatePercent = lethalStrikeRatePercent;
        this.halfKillRatePercent = halfKillRatePercent;
        this.negateRatePercent = negateRatePercent;
        this.negateAbnormalTypes = negateAbnormalTypes == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(negateAbnormalTypes));
        this.aggroPoints = aggroPoints;
        this.attribute = attribute;
        this.attributePower = attributePower;
        this.name = name;
        this.description = description;
        this.effects = effects == null ? null : Collections.unmodifiableList(new ArrayList<SkillEffect>(effects));
        this.statModifiers = statModifiers == null
                ? null
                : Collections.unmodifiableList(new ArrayList<SkillStatModifier>(statModifiers));
        this.producedItems = producedItems == null
                ? null
                : Collections.unmodifiableList(new ArrayList<SkillProducedItemGroup>(producedItems));
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

    public @Nullable Integer getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Integer getItemTemplateCount() {
        return itemTemplateCount;
    }

    /**
     * Maximum soulshot / spiritshot charges the skill consumes per cast.
     */
    public @Nullable Integer getSoulMaxConsume() {
        return soulMaxConsume;
    }

    /**
     * Energy (agathion / kamael-style resource) consumed per cast.
     */
    public @Nullable Integer getEnergyConsume() {
        return energyConsume;
    }

    /**
     * Momentum charges consumed per cast.
     */
    public @Nullable Integer getChargeConsume() {
        return chargeConsume;
    }

    public @Nullable Integer getCastRange() {
        return castRange;
    }

    public @Nullable Integer getEffectRange() {
        return effectRange;
    }

    /**
     * Radius of the area of effect for AoE skills; {@code null} for single-target.
     */
    public @Nullable Integer getAffectRange() {
        return affectRange;
    }

    /**
     * Maximum number of targets an AoE skill affects; {@code null} if unbounded / single-target.
     */
    public @Nullable Integer getAffectLimit() {
        return affectLimit;
    }

    /**
     * Fan AoE: starting angle of the arc in degrees relative to the caster's heading;
     * {@code null} for non-fan skills.
     */
    public @Nullable Integer getFanStartAngle() {
        return fanStartAngle;
    }

    /**
     * Fan AoE: arc radius; {@code null} for non-fan skills.
     */
    public @Nullable Integer getFanRadius() {
        return fanRadius;
    }

    /**
     * Fan AoE: arc width in degrees; {@code null} for non-fan skills.
     */
    public @Nullable Integer getFanAngle() {
        return fanAngle;
    }

    public @Nullable Integer getMagicLevel() {
        return magicLevel;
    }

    /**
     * Abnormal (buff/debuff) slot level — higher overwrites lower of the same type.
     */
    public @Nullable Integer getAbnormalLevel() {
        return abnormalLevel;
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
     * Base critical-hit chance of the skill before the caster's stat modifiers apply
     * (raw value as defined by the build); {@code null} / {@code 0} when the skill is
     * not crit-capable.
     */
    public @Nullable Integer getBaseCritRate() {
        return baseCritRate;
    }

    /**
     * Skill power (damage / heal magnitude) — a coefficient, no unit.
     */
    public @Nullable Double getPower() {
        return power;
    }

    /**
     * Power override used against players; {@code null} when PvP uses {@code power}.
     */
    public @Nullable Double getPvpPower() {
        return pvpPower;
    }

    /**
     * Power override used against NPCs; {@code null} when PvE uses {@code power}.
     */
    public @Nullable Double getPvePower() {
        return pvePower;
    }

    /**
     * Lower bound of the debuff land-rate after all modifiers.
     */
    public @Nullable Integer getMinChancePercent() {
        return minChancePercent;
    }

    /**
     * Upper bound of the debuff land-rate after all modifiers.
     */
    public @Nullable Integer getMaxChancePercent() {
        return maxChancePercent;
    }

    /**
     * Base activation chance the land-rate formula starts from; {@code null} when the
     * skill lands unconditionally.
     */
    public @Nullable Integer getActivateRatePercent() {
        return activateRatePercent;
    }

    /**
     * Per-level-difference modifier applied to the land-rate (caster magic level vs
     * target level).
     */
    public @Nullable Integer getLevelModifier() {
        return levelModifier;
    }

    /**
     * Chance of the full lethal strike (reduces the target to 1 HP / kills).
     */
    public @Nullable Integer getLethalStrikeRatePercent() {
        return lethalStrikeRatePercent;
    }

    /**
     * Chance of the half-kill lethal (reduces the target's HP by half).
     */
    public @Nullable Integer getHalfKillRatePercent() {
        return halfKillRatePercent;
    }

    /**
     * Success chance of removing the {@code negateAbnormalTypes} entries.
     */
    public @Nullable Integer getNegateRatePercent() {
        return negateRatePercent;
    }

    /**
     * Abnormal types this skill removes (cleanse semantics), keyed by canonical
     * UPPER_SNAKE abnormal-type token; the value is the maximum abnormal level the
     * skill can remove. {@code null} when the skill negates nothing.
     */
    public @Nullable Map<String, Integer> getNegateAbnormalTypes() {
        return negateAbnormalTypes;
    }

    /**
     * Aggro generated on NPCs by casting this level.
     */
    public @Nullable Integer getAggroPoints() {
        return aggroPoints;
    }

    /**
     * Offensive element at this level ({@code FIRE}/{@code WATER}/{@code WIND}/
     * {@code EARTH}/{@code HOLY}/{@code DARK}); {@code null} when the level carries no
     * offensive attribute.
     */
    public @Nullable String getAttribute() {
        return attribute;
    }

    /**
     * Offensive element power at this level; {@code null} when {@code attribute} is null.
     */
    public @Nullable Integer getAttributePower() {
        return attributePower;
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

    /**
     * Stat modifications attached directly to the skill (no effect wrapper — typical
     * for simple passives); {@code null} if none.
     */
    public @Nullable List<SkillStatModifier> getStatModifiers() {
        return statModifiers;
    }

    /**
     * Produced item groups for extractable skills (item-opening / conversion); {@code null}
     * when the skill produces nothing.
     */
    public @Nullable List<SkillProducedItemGroup> getProducedItems() {
        return producedItems;
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
                .soulMaxConsume(soulMaxConsume)
                .energyConsume(energyConsume)
                .chargeConsume(chargeConsume)
                .castRange(castRange)
                .effectRange(effectRange)
                .affectRange(affectRange)
                .affectLimit(affectLimit)
                .fanStartAngle(fanStartAngle)
                .fanRadius(fanRadius)
                .fanAngle(fanAngle)
                .magicLevel(magicLevel)
                .abnormalLevel(abnormalLevel)
                .abnormalTimeSec(abnormalTimeSec)
                .hitTimeMs(hitTimeMs)
                .coolTimeMs(coolTimeMs)
                .reuseDelayMs(reuseDelayMs)
                .baseCritRate(baseCritRate)
                .power(power)
                .pvpPower(pvpPower)
                .pvePower(pvePower)
                .minChancePercent(minChancePercent)
                .maxChancePercent(maxChancePercent)
                .activateRatePercent(activateRatePercent)
                .levelModifier(levelModifier)
                .lethalStrikeRatePercent(lethalStrikeRatePercent)
                .halfKillRatePercent(halfKillRatePercent)
                .negateRatePercent(negateRatePercent)
                .negateAbnormalTypes(negateAbnormalTypes)
                .aggroPoints(aggroPoints)
                .attribute(attribute)
                .attributePower(attributePower)
                .name(name)
                .description(description)
                .effects(effects)
                .statModifiers(statModifiers)
                .producedItems(producedItems);
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
                && Objects.equals(soulMaxConsume, that.soulMaxConsume)
                && Objects.equals(energyConsume, that.energyConsume)
                && Objects.equals(chargeConsume, that.chargeConsume)
                && Objects.equals(castRange, that.castRange)
                && Objects.equals(effectRange, that.effectRange)
                && Objects.equals(affectRange, that.affectRange)
                && Objects.equals(affectLimit, that.affectLimit)
                && Objects.equals(fanStartAngle, that.fanStartAngle)
                && Objects.equals(fanRadius, that.fanRadius)
                && Objects.equals(fanAngle, that.fanAngle)
                && Objects.equals(magicLevel, that.magicLevel)
                && Objects.equals(abnormalLevel, that.abnormalLevel)
                && Objects.equals(abnormalTimeSec, that.abnormalTimeSec)
                && Objects.equals(hitTimeMs, that.hitTimeMs)
                && Objects.equals(coolTimeMs, that.coolTimeMs)
                && Objects.equals(reuseDelayMs, that.reuseDelayMs)
                && Objects.equals(baseCritRate, that.baseCritRate)
                && Objects.equals(power, that.power)
                && Objects.equals(pvpPower, that.pvpPower)
                && Objects.equals(pvePower, that.pvePower)
                && Objects.equals(minChancePercent, that.minChancePercent)
                && Objects.equals(maxChancePercent, that.maxChancePercent)
                && Objects.equals(activateRatePercent, that.activateRatePercent)
                && Objects.equals(levelModifier, that.levelModifier)
                && Objects.equals(lethalStrikeRatePercent, that.lethalStrikeRatePercent)
                && Objects.equals(halfKillRatePercent, that.halfKillRatePercent)
                && Objects.equals(negateRatePercent, that.negateRatePercent)
                && Objects.equals(negateAbnormalTypes, that.negateAbnormalTypes)
                && Objects.equals(aggroPoints, that.aggroPoints)
                && Objects.equals(attribute, that.attribute)
                && Objects.equals(attributePower, that.attributePower)
                && Objects.equals(name, that.name)
                && Objects.equals(description, that.description)
                && Objects.equals(effects, that.effects)
                && Objects.equals(statModifiers, that.statModifiers)
                && Objects.equals(producedItems, that.producedItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                level,
                icon,
                mpConsume,
                mpInitialConsume,
                hpConsume,
                itemTemplateId,
                itemTemplateCount,
                soulMaxConsume,
                energyConsume,
                chargeConsume,
                castRange,
                effectRange,
                affectRange,
                affectLimit,
                fanStartAngle,
                fanRadius,
                fanAngle,
                magicLevel,
                abnormalLevel,
                abnormalTimeSec,
                hitTimeMs,
                coolTimeMs,
                reuseDelayMs,
                baseCritRate,
                power,
                pvpPower,
                pvePower,
                minChancePercent,
                maxChancePercent,
                activateRatePercent,
                levelModifier,
                lethalStrikeRatePercent,
                halfKillRatePercent,
                negateRatePercent,
                negateAbnormalTypes,
                aggroPoints,
                attribute,
                attributePower,
                name,
                description,
                effects,
                statModifiers,
                producedItems);
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
        private @Nullable Integer soulMaxConsume;
        private @Nullable Integer energyConsume;
        private @Nullable Integer chargeConsume;
        private @Nullable Integer castRange;
        private @Nullable Integer effectRange;
        private @Nullable Integer affectRange;
        private @Nullable Integer affectLimit;
        private @Nullable Integer fanStartAngle;
        private @Nullable Integer fanRadius;
        private @Nullable Integer fanAngle;
        private @Nullable Integer magicLevel;
        private @Nullable Integer abnormalLevel;
        private @Nullable Integer abnormalTimeSec;
        private @Nullable Integer hitTimeMs;
        private @Nullable Integer coolTimeMs;
        private @Nullable Integer reuseDelayMs;
        private @Nullable Integer baseCritRate;
        private @Nullable Double power;
        private @Nullable Double pvpPower;
        private @Nullable Double pvePower;
        private @Nullable Integer minChancePercent;
        private @Nullable Integer maxChancePercent;
        private @Nullable Integer activateRatePercent;
        private @Nullable Integer levelModifier;
        private @Nullable Integer lethalStrikeRatePercent;
        private @Nullable Integer halfKillRatePercent;
        private @Nullable Integer negateRatePercent;
        private @Nullable Map<String, Integer> negateAbnormalTypes;
        private @Nullable Integer aggroPoints;
        private @Nullable String attribute;
        private @Nullable Integer attributePower;
        private @Nullable LocalizedText name;
        private @Nullable LocalizedText description;
        private @Nullable List<SkillEffect> effects;
        private @Nullable List<SkillStatModifier> statModifiers;
        private @Nullable List<SkillProducedItemGroup> producedItems;

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

        public Builder soulMaxConsume(@Nullable Integer soulMaxConsume) {
            this.soulMaxConsume = soulMaxConsume;
            return this;
        }

        public Builder energyConsume(@Nullable Integer energyConsume) {
            this.energyConsume = energyConsume;
            return this;
        }

        public Builder chargeConsume(@Nullable Integer chargeConsume) {
            this.chargeConsume = chargeConsume;
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

        public Builder affectRange(@Nullable Integer affectRange) {
            this.affectRange = affectRange;
            return this;
        }

        public Builder affectLimit(@Nullable Integer affectLimit) {
            this.affectLimit = affectLimit;
            return this;
        }

        public Builder fanStartAngle(@Nullable Integer fanStartAngle) {
            this.fanStartAngle = fanStartAngle;
            return this;
        }

        public Builder fanRadius(@Nullable Integer fanRadius) {
            this.fanRadius = fanRadius;
            return this;
        }

        public Builder fanAngle(@Nullable Integer fanAngle) {
            this.fanAngle = fanAngle;
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

        public Builder baseCritRate(@Nullable Integer baseCritRate) {
            this.baseCritRate = baseCritRate;
            return this;
        }

        public Builder power(@Nullable Double power) {
            this.power = power;
            return this;
        }

        public Builder pvpPower(@Nullable Double pvpPower) {
            this.pvpPower = pvpPower;
            return this;
        }

        public Builder pvePower(@Nullable Double pvePower) {
            this.pvePower = pvePower;
            return this;
        }

        public Builder minChancePercent(@Nullable Integer minChancePercent) {
            this.minChancePercent = minChancePercent;
            return this;
        }

        public Builder maxChancePercent(@Nullable Integer maxChancePercent) {
            this.maxChancePercent = maxChancePercent;
            return this;
        }

        public Builder activateRatePercent(@Nullable Integer activateRatePercent) {
            this.activateRatePercent = activateRatePercent;
            return this;
        }

        public Builder levelModifier(@Nullable Integer levelModifier) {
            this.levelModifier = levelModifier;
            return this;
        }

        public Builder lethalStrikeRatePercent(@Nullable Integer lethalStrikeRatePercent) {
            this.lethalStrikeRatePercent = lethalStrikeRatePercent;
            return this;
        }

        public Builder halfKillRatePercent(@Nullable Integer halfKillRatePercent) {
            this.halfKillRatePercent = halfKillRatePercent;
            return this;
        }

        public Builder negateRatePercent(@Nullable Integer negateRatePercent) {
            this.negateRatePercent = negateRatePercent;
            return this;
        }

        public Builder negateAbnormalTypes(@Nullable Map<String, Integer> negateAbnormalTypes) {
            this.negateAbnormalTypes = negateAbnormalTypes;
            return this;
        }

        public Builder aggroPoints(@Nullable Integer aggroPoints) {
            this.aggroPoints = aggroPoints;
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

        public Builder statModifiers(@Nullable List<SkillStatModifier> statModifiers) {
            this.statModifiers = statModifiers;
            return this;
        }

        public Builder producedItems(@Nullable List<SkillProducedItemGroup> producedItems) {
            this.producedItems = producedItems;
            return this;
        }

        public SkillLevel build() {
            return new SkillLevel(
                    level,
                    icon,
                    mpConsume,
                    mpInitialConsume,
                    hpConsume,
                    itemTemplateId,
                    itemTemplateCount,
                    soulMaxConsume,
                    energyConsume,
                    chargeConsume,
                    castRange,
                    effectRange,
                    affectRange,
                    affectLimit,
                    fanStartAngle,
                    fanRadius,
                    fanAngle,
                    magicLevel,
                    abnormalLevel,
                    abnormalTimeSec,
                    hitTimeMs,
                    coolTimeMs,
                    reuseDelayMs,
                    baseCritRate,
                    power,
                    pvpPower,
                    pvePower,
                    minChancePercent,
                    maxChancePercent,
                    activateRatePercent,
                    levelModifier,
                    lethalStrikeRatePercent,
                    halfKillRatePercent,
                    negateRatePercent,
                    negateAbnormalTypes,
                    aggroPoints,
                    attribute,
                    attributePower,
                    name,
                    description,
                    effects,
                    statModifiers,
                    producedItems);
        }
    }
}
