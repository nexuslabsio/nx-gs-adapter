package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Build-agnostic skill wire DTO — the common L2 denominator for static skill data,
 * carried as the payload of {@code GameDataSyncEvent} on the {@code gd} (game-data)
 * sync stream's {@code skill} entity topic. Each host build supplies its own provider
 * that maps its core's internal skill representation into this shape; nothing here
 * names a specific core.
 *
 * <p>One {@code SkillTemplate} is the whole aggregate for a {@code skillId}: the level-invariant
 * header plus the nested level ladder ({@link #getLevels()}) and enchant-route variants
 * ({@link #getEnchantRoutes()}). The consumer upserts the parent and replaces its
 * children atomically.</p>
 *
 * <p><b>Nullability:</b> only {@link #getId()} is non-null. Every other field is
 * {@link Nullable} (former primitives boxed) so {@code null} means "this build did not
 * supply it" rather than a fabricated default.</p>
 *
 * <p><b>Vocabulary:</b> {@code operateType} / {@code skillType} / {@code targetType} /
 * {@code trait} / {@code attribute} / {@code abnormalType} / {@code saveVs} are open
 * canonical L2 strings (the value sets are large and fork-variable; the provider emits
 * the core's enum name / attribute name, not a JVM ordinal). Boolean classification
 * flags are grouped in {@link #getFlags()} ({@link SkillFlags}), unwrapped flat into
 * columns by the consumer.</p>
 */
public final class SkillTemplate {

    private final int id;
    private final @Nullable String operateType;
    private final @Nullable String skillType;
    private final @Nullable String targetType;
    private final @Nullable String trait;
    private final @Nullable String attribute;
    private final @Nullable Integer attributePower;
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

    public SkillTemplate(int id,
                         @Nullable String operateType,
                         @Nullable String skillType,
                         @Nullable String targetType,
                         @Nullable String trait,
                         @Nullable String attribute,
                         @Nullable Integer attributePower,
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
                         @Nullable List<SkillClassLearn> classes) {
        this.id = id;
        this.operateType = operateType;
        this.skillType = skillType;
        this.targetType = targetType;
        this.trait = trait;
        this.attribute = attribute;
        this.attributePower = attributePower;
        this.abnormalType = abnormalType;
        this.abnormalVisualEffects = abnormalVisualEffects == null ? null
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
        this.conditions = conditions == null ? null
                : Collections.unmodifiableList(new ArrayList<SkillCondition>(conditions));
        this.levels = levels == null ? null
                : Collections.unmodifiableList(new ArrayList<SkillLevel>(levels));
        this.enchantRoutes = enchantRoutes == null ? null
                : Collections.unmodifiableList(new ArrayList<SkillEnchantRoute>(enchantRoutes));
        this.classes = classes == null ? null
                : Collections.unmodifiableList(new ArrayList<SkillClassLearn>(classes));
    }

    public int getId() {
        return id;
    }

    /**
     * Operate-type — canonical L2 code (e.g. {@code A1}, {@code CA1}, {@code DA2},
     * {@code TG}, {@code AU}). Active / continuous-active / delayed-active / toggle / aura.
     */
    public @Nullable String getOperateType() {
        return operateType;
    }

    /**
     * SkillTemplate type (e.g. {@code PDAM}, {@code BUFF}, {@code DEBUFF}, {@code HEAL}).
     */
    public @Nullable String getSkillType() {
        return skillType;
    }

    /**
     * Target type (e.g. {@code ENEMY_ONLY}, {@code SELF}, {@code PARTY}).
     */
    public @Nullable String getTargetType() {
        return targetType;
    }

    /**
     * Trait (e.g. {@code BLEED}, {@code POISON}, {@code NONE}).
     */
    public @Nullable String getTrait() {
        return trait;
    }

    /**
     * Attack attribute name ({@code FIRE}/{@code WATER}/{@code WIND}/{@code EARTH}/
     * {@code HOLY}/{@code DARK}); {@code null} when the skill has no attribute.
     */
    public @Nullable String getAttribute() {
        return attribute;
    }

    public @Nullable Integer getAttributePower() {
        return attributePower;
    }

    /**
     * Abnormal (buff-slot) stacking type of the skill's primary effect — canonical
     * UPPER_SNAKE token; same-type abnormals overwrite by abnormal level. {@code null}
     * when the skill occupies no buff slot.
     */
    public @Nullable String getAbnormalType() {
        return abnormalType;
    }

    /**
     * Client visual effect tokens shown while the abnormal is active (canonical
     * UPPER_SNAKE, e.g. {@code STUN}, {@code POISON}); {@code null} when none.
     */
    public @Nullable List<String> getAbnormalVisualEffects() {
        return abnormalVisualEffects;
    }

    /**
     * Saving stat the land-rate formula rolls against ({@code STR}/{@code CON}/
     * {@code DEX}/{@code INT}/{@code WIT}/{@code MEN}); {@code null} when the skill
     * makes no save roll.
     */
    public @Nullable String getSaveVs() {
        return saveVs;
    }

    /**
     * Shared-cooldown group id — skills with the same group share their reuse delay;
     * {@code null} when the skill cools down independently.
     */
    public @Nullable Integer getSharedReuseGroup() {
        return sharedReuseGroup;
    }

    /**
     * Minimum pledge (clan) rank required to cast; {@code null} when unrestricted.
     */
    public @Nullable Integer getMinPledgeClass() {
        return minPledgeClass;
    }

    /**
     * Skill id this skill triggers on proc; {@code null} when the skill triggers nothing.
     */
    public @Nullable Integer getTriggeredSkillId() {
        return triggeredSkillId;
    }

    /**
     * Level of the triggered skill; {@code null} when {@code triggeredSkillId} is null.
     */
    public @Nullable Integer getTriggeredSkillLevel() {
        return triggeredSkillLevel;
    }

    /**
     * Event that fires the trigger — canonical UPPER_SNAKE token (e.g. {@code ON_HIT},
     * {@code ON_CRIT}, {@code ON_ATTACKED}); {@code null} when the skill triggers
     * unconditionally or triggers nothing.
     */
    public @Nullable String getTriggeredChanceType() {
        return triggeredChanceType;
    }

    /**
     * Chance of the trigger firing on the {@code triggeredChanceType} event; {@code null}
     * when the build defines no chance.
     */
    public @Nullable Integer getTriggeredChancePercent() {
        return triggeredChancePercent;
    }

    public @Nullable String getIcon() {
        return icon;
    }

    /**
     * Number of base levels in the ladder.
     */
    public @Nullable Integer getMaxLevel() {
        return maxLevel;
    }

    /**
     * Boolean classification flags (magic / debuff / passive / …); {@code null} if none supplied.
     */
    public @Nullable SkillFlags getFlags() {
        return flags;
    }

    /**
     * Cast preconditions (weapon / target / state requirements), read from the skill's
     * canonical level; {@code null} if none.
     */
    public @Nullable List<SkillCondition> getConditions() {
        return conditions;
    }

    /**
     * Base level ladder; {@code null} if not supplied (empty is a degenerate skill).
     */
    public @Nullable List<SkillLevel> getLevels() {
        return levels;
    }

    /**
     * Enchant-route variants (enchanted levels beyond the base ladder); {@code null} if none.
     */
    public @Nullable List<SkillEnchantRoute> getEnchantRoutes() {
        return enchantRoutes;
    }

    /**
     * Playable classes that learn this skill (inverted from the host's class skill trees);
     * {@code null} if none (e.g. NPC-only / item-granted skills).
     */
    public @Nullable List<SkillClassLearn> getClasses() {
        return classes;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .operateType(operateType)
                .skillType(skillType)
                .targetType(targetType)
                .trait(trait)
                .attribute(attribute)
                .attributePower(attributePower)
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
                .classes(classes);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillTemplate)) return false;
        SkillTemplate that = (SkillTemplate) o;
        return id == that.id
                && Objects.equals(operateType, that.operateType)
                && Objects.equals(skillType, that.skillType)
                && Objects.equals(targetType, that.targetType)
                && Objects.equals(trait, that.trait)
                && Objects.equals(attribute, that.attribute)
                && Objects.equals(attributePower, that.attributePower)
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
                && Objects.equals(classes, that.classes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, operateType, skillType, targetType, trait, attribute,
                attributePower, abnormalType, abnormalVisualEffects, saveVs, sharedReuseGroup,
                minPledgeClass, triggeredSkillId, triggeredSkillLevel, triggeredChanceType,
                triggeredChancePercent, icon, maxLevel, flags, conditions, levels, enchantRoutes,
                classes);
    }

    @Override
    public String toString() {
        return "SkillTemplate[id=" + id + ", skillType=" + skillType + ", maxLevel=" + maxLevel + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable String operateType;
        private @Nullable String skillType;
        private @Nullable String targetType;
        private @Nullable String trait;
        private @Nullable String attribute;
        private @Nullable Integer attributePower;
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

        public Builder attribute(@Nullable String attribute) {
            this.attribute = attribute;
            return this;
        }

        public Builder attributePower(@Nullable Integer attributePower) {
            this.attributePower = attributePower;
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

        public SkillTemplate build() {
            return new SkillTemplate(id, operateType, skillType, targetType, trait, attribute,
                    attributePower, abnormalType, abnormalVisualEffects, saveVs, sharedReuseGroup,
                    minPledgeClass, triggeredSkillId, triggeredSkillLevel, triggeredChanceType,
                    triggeredChancePercent, icon, maxLevel, flags, conditions, levels,
                    enchantRoutes, classes);
        }
    }
}
