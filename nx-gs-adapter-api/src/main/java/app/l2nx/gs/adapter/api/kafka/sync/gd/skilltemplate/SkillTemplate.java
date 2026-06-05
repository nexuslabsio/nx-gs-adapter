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
 * {@code trait} / {@code element} are open canonical L2 strings (the value sets are
 * large and fork-variable; the provider emits the core's enum name / element name, not
 * a JVM ordinal). Boolean classification flags are grouped in {@link #getFlags()}
 * ({@link SkillFlags}), unwrapped flat into columns by the consumer.</p>
 */
public final class SkillTemplate {

    private final int id;
    private final @Nullable String operateType;
    private final @Nullable String skillType;
    private final @Nullable String targetType;
    private final @Nullable String trait;
    private final @Nullable String element;
    private final @Nullable Integer elementPower;
    private final @Nullable String icon;
    private final @Nullable Integer maxLevel;
    private final @Nullable SkillFlags flags;
    private final @Nullable List<SkillLevel> levels;
    private final @Nullable List<SkillEnchantRoute> enchantRoutes;
    private final @Nullable List<SkillClassLearn> classes;

    public SkillTemplate(int id,
                         @Nullable String operateType,
                         @Nullable String skillType,
                         @Nullable String targetType,
                         @Nullable String trait,
                         @Nullable String element,
                         @Nullable Integer elementPower,
                         @Nullable String icon,
                         @Nullable Integer maxLevel,
                         @Nullable SkillFlags flags,
                         @Nullable List<SkillLevel> levels,
                         @Nullable List<SkillEnchantRoute> enchantRoutes,
                         @Nullable List<SkillClassLearn> classes) {
        this.id = id;
        this.operateType = operateType;
        this.skillType = skillType;
        this.targetType = targetType;
        this.trait = trait;
        this.element = element;
        this.elementPower = elementPower;
        this.icon = icon;
        this.maxLevel = maxLevel;
        this.flags = flags;
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
     * Attack element name ({@code FIRE}/{@code WATER}/{@code WIND}/{@code EARTH}/
     * {@code HOLY}/{@code DARK}/{@code NONE}).
     */
    public @Nullable String getElement() {
        return element;
    }

    public @Nullable Integer getElementPower() {
        return elementPower;
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
                .element(element)
                .elementPower(elementPower)
                .icon(icon)
                .maxLevel(maxLevel)
                .flags(flags)
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
                && Objects.equals(element, that.element)
                && Objects.equals(elementPower, that.elementPower)
                && Objects.equals(icon, that.icon)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(flags, that.flags)
                && Objects.equals(levels, that.levels)
                && Objects.equals(enchantRoutes, that.enchantRoutes)
                && Objects.equals(classes, that.classes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, operateType, skillType, targetType, trait, element, elementPower,
                icon, maxLevel, flags, levels, enchantRoutes, classes);
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
        private @Nullable String element;
        private @Nullable Integer elementPower;
        private @Nullable String icon;
        private @Nullable Integer maxLevel;
        private @Nullable SkillFlags flags;
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

        public Builder element(@Nullable String element) {
            this.element = element;
            return this;
        }

        public Builder elementPower(@Nullable Integer elementPower) {
            this.elementPower = elementPower;
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
            return new SkillTemplate(id, operateType, skillType, targetType, trait, element,
                    elementPower, icon, maxLevel, flags, levels, enchantRoutes, classes);
        }
    }
}
