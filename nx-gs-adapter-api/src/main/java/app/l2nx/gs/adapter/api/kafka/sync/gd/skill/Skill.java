package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Build-agnostic skill wire DTO ({@code skill} topic of the {@code gd} sync stream); one instance is the whole
 * aggregate for a skillId. Only {@link #getId()} is non-null: {@code null} means the build did not supply the value.
 * Type-like strings are open canonical L2 names (enum name, not ordinal); the offensive attribute lives on
 * {@link SkillLevel} and {@link SkillEnchantRoute}.
 */
public final class Skill {

    private final int id;
    private final @Nullable String operateType;
    private final @Nullable String skillType;
    private final @Nullable String targetType;
    private final @Nullable String trait;
    private final @Nullable String abnormalType;
    private final @Nullable List<String> abnormalVisualEffects;
    private final @Nullable String saveVs;
    private final @Nullable Integer sharedReuseGroup;
    private final @Nullable Integer minPledgeClass;
    private final @Nullable Integer triggeredSkillId;
    private final @Nullable Integer triggeredSkillLevel;
    private final @Nullable String triggeredChanceType;
    private final @Nullable Integer triggeredChancePercent;
    private final @Nullable String icon;
    private final @Nullable Integer maxLevel;
    private final @Nullable SkillFlags flags;
    private final @Nullable List<SkillCondition> conditions;
    private final @Nullable List<SkillLevel> levels;
    private final @Nullable List<SkillEnchantRoute> enchantRoutes;
    private final @Nullable List<SkillClassLearn> classes;
    private final @Nullable List<GearScoreContribution> gearScoreContributions;

    public Skill(
            int id,
            @Nullable String operateType,
            @Nullable String skillType,
            @Nullable String targetType,
            @Nullable String trait,
            @Nullable String abnormalType,
            @Nullable List<String> abnormalVisualEffects,
            @Nullable String saveVs,
            @Nullable Integer sharedReuseGroup,
            @Nullable Integer minPledgeClass,
            @Nullable Integer triggeredSkillId,
            @Nullable Integer triggeredSkillLevel,
            @Nullable String triggeredChanceType,
            @Nullable Integer triggeredChancePercent,
            @Nullable String icon,
            @Nullable Integer maxLevel,
            @Nullable SkillFlags flags,
            @Nullable List<SkillCondition> conditions,
            @Nullable List<SkillLevel> levels,
            @Nullable List<SkillEnchantRoute> enchantRoutes,
            @Nullable List<SkillClassLearn> classes,
            @Nullable List<GearScoreContribution> gearScoreContributions) {
        this.id = id;
        this.operateType = operateType;
        this.skillType = skillType;
        this.targetType = targetType;
        this.trait = trait;
        this.abnormalType = abnormalType;
        this.abnormalVisualEffects = abnormalVisualEffects == null
                ? null
                : Collections.unmodifiableList(new ArrayList<String>(abnormalVisualEffects));
        this.saveVs = saveVs;
        this.sharedReuseGroup = sharedReuseGroup;
        this.minPledgeClass = minPledgeClass;
        this.triggeredSkillId = triggeredSkillId;
        this.triggeredSkillLevel = triggeredSkillLevel;
        this.triggeredChanceType = triggeredChanceType;
        this.triggeredChancePercent = triggeredChancePercent;
        this.icon = icon;
        this.maxLevel = maxLevel;
        this.flags = flags;
        this.conditions =
                conditions == null ? null : Collections.unmodifiableList(new ArrayList<SkillCondition>(conditions));
        this.levels = levels == null ? null : Collections.unmodifiableList(new ArrayList<SkillLevel>(levels));
        this.enchantRoutes = enchantRoutes == null
                ? null
                : Collections.unmodifiableList(new ArrayList<SkillEnchantRoute>(enchantRoutes));
        this.classes = classes == null ? null : Collections.unmodifiableList(new ArrayList<SkillClassLearn>(classes));
        this.gearScoreContributions = gearScoreContributions == null
                ? null
                : Collections.unmodifiableList(new ArrayList<GearScoreContribution>(gearScoreContributions));
    }

    public int getId() {
        return id;
    }

    /** Code: {@code A1}, {@code CA1}, {@code DA2}, {@code TG}, {@code AU}. */
    public @Nullable String getOperateType() {
        return operateType;
    }

    public @Nullable String getSkillType() {
        return skillType;
    }

    public @Nullable String getTargetType() {
        return targetType;
    }

    public @Nullable String getTrait() {
        return trait;
    }

    /**
     * Buff-slot stacking type of the primary effect; same-type abnormals overwrite by abnormal level.
     * {@code null} when the skill occupies no buff slot.
     */
    public @Nullable String getAbnormalType() {
        return abnormalType;
    }

    public @Nullable List<String> getAbnormalVisualEffects() {
        return abnormalVisualEffects;
    }

    /** STR / CON / DEX / INT / WIT / MEN; {@code null} when the skill makes no save roll. */
    public @Nullable String getSaveVs() {
        return saveVs;
    }

    /** Skills in the same group share their reuse delay; {@code null} when independent. */
    public @Nullable Integer getSharedReuseGroup() {
        return sharedReuseGroup;
    }

    public @Nullable Integer getMinPledgeClass() {
        return minPledgeClass;
    }

    public @Nullable Integer getTriggeredSkillId() {
        return triggeredSkillId;
    }

    public @Nullable Integer getTriggeredSkillLevel() {
        return triggeredSkillLevel;
    }

    /** Token such as {@code ON_HIT} / {@code ON_CRIT}; {@code null} when unconditional or no trigger. */
    public @Nullable String getTriggeredChanceType() {
        return triggeredChanceType;
    }

    public @Nullable Integer getTriggeredChancePercent() {
        return triggeredChancePercent;
    }

    public @Nullable String getIcon() {
        return icon;
    }

    public @Nullable Integer getMaxLevel() {
        return maxLevel;
    }

    public @Nullable SkillFlags getFlags() {
        return flags;
    }

    /** Read from the skill's canonical level. */
    public @Nullable List<SkillCondition> getConditions() {
        return conditions;
    }

    public @Nullable List<SkillLevel> getLevels() {
        return levels;
    }

    public @Nullable List<SkillEnchantRoute> getEnchantRoutes() {
        return enchantRoutes;
    }

    /** Inverted from the host's class skill trees; {@code null} for NPC-only / item-granted skills. */
    public @Nullable List<SkillClassLearn> getClasses() {
        return classes;
    }

    /** Owning / per-level / per-enchant bonuses; {@code null} when the build computes no gear score. */
    public @Nullable List<GearScoreContribution> getGearScoreContributions() {
        return gearScoreContributions;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .operateType(operateType)
                .skillType(skillType)
                .targetType(targetType)
                .trait(trait)
                .abnormalType(abnormalType)
                .abnormalVisualEffects(abnormalVisualEffects)
                .saveVs(saveVs)
                .sharedReuseGroup(sharedReuseGroup)
                .minPledgeClass(minPledgeClass)
                .triggeredSkillId(triggeredSkillId)
                .triggeredSkillLevel(triggeredSkillLevel)
                .triggeredChanceType(triggeredChanceType)
                .triggeredChancePercent(triggeredChancePercent)
                .icon(icon)
                .maxLevel(maxLevel)
                .flags(flags)
                .conditions(conditions)
                .levels(levels)
                .enchantRoutes(enchantRoutes)
                .classes(classes)
                .gearScoreContributions(gearScoreContributions);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Skill)) return false;
        Skill that = (Skill) o;
        return id == that.id
                && Objects.equals(operateType, that.operateType)
                && Objects.equals(skillType, that.skillType)
                && Objects.equals(targetType, that.targetType)
                && Objects.equals(trait, that.trait)
                && Objects.equals(abnormalType, that.abnormalType)
                && Objects.equals(abnormalVisualEffects, that.abnormalVisualEffects)
                && Objects.equals(saveVs, that.saveVs)
                && Objects.equals(sharedReuseGroup, that.sharedReuseGroup)
                && Objects.equals(minPledgeClass, that.minPledgeClass)
                && Objects.equals(triggeredSkillId, that.triggeredSkillId)
                && Objects.equals(triggeredSkillLevel, that.triggeredSkillLevel)
                && Objects.equals(triggeredChanceType, that.triggeredChanceType)
                && Objects.equals(triggeredChancePercent, that.triggeredChancePercent)
                && Objects.equals(icon, that.icon)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(flags, that.flags)
                && Objects.equals(conditions, that.conditions)
                && Objects.equals(levels, that.levels)
                && Objects.equals(enchantRoutes, that.enchantRoutes)
                && Objects.equals(classes, that.classes)
                && Objects.equals(gearScoreContributions, that.gearScoreContributions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                operateType,
                skillType,
                targetType,
                trait,
                abnormalType,
                abnormalVisualEffects,
                saveVs,
                sharedReuseGroup,
                minPledgeClass,
                triggeredSkillId,
                triggeredSkillLevel,
                triggeredChanceType,
                triggeredChancePercent,
                icon,
                maxLevel,
                flags,
                conditions,
                levels,
                enchantRoutes,
                classes,
                gearScoreContributions);
    }

    @Override
    public String toString() {
        return "Skill[id=" + id + ", skillType=" + skillType + ", maxLevel=" + maxLevel + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable String operateType;
        private @Nullable String skillType;
        private @Nullable String targetType;
        private @Nullable String trait;
        private @Nullable String abnormalType;
        private @Nullable List<String> abnormalVisualEffects;
        private @Nullable String saveVs;
        private @Nullable Integer sharedReuseGroup;
        private @Nullable Integer minPledgeClass;
        private @Nullable Integer triggeredSkillId;
        private @Nullable Integer triggeredSkillLevel;
        private @Nullable String triggeredChanceType;
        private @Nullable Integer triggeredChancePercent;
        private @Nullable String icon;
        private @Nullable Integer maxLevel;
        private @Nullable SkillFlags flags;
        private @Nullable List<SkillCondition> conditions;
        private @Nullable List<SkillLevel> levels;
        private @Nullable List<SkillEnchantRoute> enchantRoutes;
        private @Nullable List<SkillClassLearn> classes;
        private @Nullable List<GearScoreContribution> gearScoreContributions;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder operateType(@Nullable String operateType) {
            this.operateType = operateType;
            return this;
        }

        public Builder skillType(@Nullable String skillType) {
            this.skillType = skillType;
            return this;
        }

        public Builder targetType(@Nullable String targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder trait(@Nullable String trait) {
            this.trait = trait;
            return this;
        }

        public Builder abnormalType(@Nullable String abnormalType) {
            this.abnormalType = abnormalType;
            return this;
        }

        public Builder abnormalVisualEffects(@Nullable List<String> abnormalVisualEffects) {
            this.abnormalVisualEffects = abnormalVisualEffects;
            return this;
        }

        public Builder saveVs(@Nullable String saveVs) {
            this.saveVs = saveVs;
            return this;
        }

        public Builder sharedReuseGroup(@Nullable Integer sharedReuseGroup) {
            this.sharedReuseGroup = sharedReuseGroup;
            return this;
        }

        public Builder minPledgeClass(@Nullable Integer minPledgeClass) {
            this.minPledgeClass = minPledgeClass;
            return this;
        }

        public Builder triggeredSkillId(@Nullable Integer triggeredSkillId) {
            this.triggeredSkillId = triggeredSkillId;
            return this;
        }

        public Builder triggeredSkillLevel(@Nullable Integer triggeredSkillLevel) {
            this.triggeredSkillLevel = triggeredSkillLevel;
            return this;
        }

        public Builder triggeredChanceType(@Nullable String triggeredChanceType) {
            this.triggeredChanceType = triggeredChanceType;
            return this;
        }

        public Builder triggeredChancePercent(@Nullable Integer triggeredChancePercent) {
            this.triggeredChancePercent = triggeredChancePercent;
            return this;
        }

        public Builder icon(@Nullable String icon) {
            this.icon = icon;
            return this;
        }

        public Builder maxLevel(@Nullable Integer maxLevel) {
            this.maxLevel = maxLevel;
            return this;
        }

        public Builder flags(@Nullable SkillFlags flags) {
            this.flags = flags;
            return this;
        }

        public Builder conditions(@Nullable List<SkillCondition> conditions) {
            this.conditions = conditions;
            return this;
        }

        public Builder levels(@Nullable List<SkillLevel> levels) {
            this.levels = levels;
            return this;
        }

        public Builder enchantRoutes(@Nullable List<SkillEnchantRoute> enchantRoutes) {
            this.enchantRoutes = enchantRoutes;
            return this;
        }

        public Builder classes(@Nullable List<SkillClassLearn> classes) {
            this.classes = classes;
            return this;
        }

        public Builder gearScoreContributions(@Nullable List<GearScoreContribution> gearScoreContributions) {
            this.gearScoreContributions = gearScoreContributions;
            return this;
        }

        public Skill build() {
            return new Skill(
                    id,
                    operateType,
                    skillType,
                    targetType,
                    trait,
                    abnormalType,
                    abnormalVisualEffects,
                    saveVs,
                    sharedReuseGroup,
                    minPledgeClass,
                    triggeredSkillId,
                    triggeredSkillLevel,
                    triggeredChanceType,
                    triggeredChancePercent,
                    icon,
                    maxLevel,
                    flags,
                    conditions,
                    levels,
                    enchantRoutes,
                    classes,
                    gearScoreContributions);
        }
    }
}
