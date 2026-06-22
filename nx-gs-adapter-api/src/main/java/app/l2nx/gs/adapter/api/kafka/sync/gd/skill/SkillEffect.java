package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One effect a {@link SkillLevel} applies — the build-agnostic projection of a core
 * effect template. {@code name} is the effect handler name (e.g. {@code p_attack},
 * {@code Stun}, {@code HealPercent}); {@code params} are its tuning parameters as a
 * flat string→string map (the host flattens the core's parameter set, value-typed
 * however the core stores it).
 *
 * <p>{@code name} is the non-null identity. Effects are per-level — each skill level
 * carries its own list. The full numeric variation across levels is also captured by
 * the typed {@link SkillLevel} columns ({@code power}, {@code abnormalTimeSec}); this
 * carries the qualitative "which handlers fire".</p>
 *
 * <p>{@code kind} tells which effect channel the entry belongs to: {@code TARGET}
 * (applied to the skill's target — the default), {@code SELF} (applied to the caster on
 * cast) or {@code PASSIVE} (constantly applied while the passive / toggle is active).
 * {@code null} means {@code TARGET} (pre-{@code kind} wire back-compat).</p>
 *
 * <p>{@code abnormalType} / {@code abnormalLevel} are the buff-slot stacking
 * coordinates of this effect (same-type abnormals overwrite by level);
 * {@code effectPower} is the effect's own magnitude operand (land-rate / value base —
 * distinct from the skill-level {@code power}). {@code statModifiers} are the stat
 * modifications this effect applies while active — the substance of buffs and passives.</p>
 */
public final class SkillEffect {

    private final String name;
    private final @Nullable String kind;
    private final @Nullable Map<String, String> params;
    private final @Nullable String abnormalType;
    private final @Nullable Integer abnormalLevel;
    private final @Nullable Double effectPower;
    private final @Nullable List<SkillStatModifier> statModifiers;

    public SkillEffect(
            String name,
            @Nullable String kind,
            @Nullable Map<String, String> params,
            @Nullable String abnormalType,
            @Nullable Integer abnormalLevel,
            @Nullable Double effectPower,
            @Nullable List<SkillStatModifier> statModifiers) {
        this.name = Objects.requireNonNull(name, "name");
        this.kind = kind;
        this.params = params == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(params));
        this.abnormalType = abnormalType;
        this.abnormalLevel = abnormalLevel;
        this.effectPower = effectPower;
        this.statModifiers = statModifiers == null
                ? null
                : Collections.unmodifiableList(new ArrayList<SkillStatModifier>(statModifiers));
    }

    public String getName() {
        return name;
    }

    /**
     * Effect channel — {@code TARGET} / {@code SELF} / {@code PASSIVE}; {@code null}
     * means {@code TARGET}.
     */
    public @Nullable String getKind() {
        return kind;
    }

    /**
     * Effect handler parameters as a flat string-keyed map; {@code null} when the
     * handler takes none.
     */
    public @Nullable Map<String, String> getParams() {
        return params;
    }

    /**
     * Abnormal (buff-slot) stacking type — canonical UPPER_SNAKE token; {@code null}
     * when the effect occupies no buff slot.
     */
    public @Nullable String getAbnormalType() {
        return abnormalType;
    }

    /**
     * Abnormal stacking level within {@code abnormalType} (higher overwrites lower).
     */
    public @Nullable Integer getAbnormalLevel() {
        return abnormalLevel;
    }

    /**
     * The effect's own magnitude operand (land-rate / value base); distinct from the
     * skill-level {@code power}.
     */
    public @Nullable Double getEffectPower() {
        return effectPower;
    }

    /**
     * Stat modifications applied while the effect is active; {@code null} if none.
     */
    public @Nullable List<SkillStatModifier> getStatModifiers() {
        return statModifiers;
    }

    public Builder toBuilder() {
        return new Builder()
                .name(name)
                .kind(kind)
                .params(params)
                .abnormalType(abnormalType)
                .abnormalLevel(abnormalLevel)
                .effectPower(effectPower)
                .statModifiers(statModifiers);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillEffect)) return false;
        SkillEffect that = (SkillEffect) o;
        return Objects.equals(name, that.name)
                && Objects.equals(kind, that.kind)
                && Objects.equals(params, that.params)
                && Objects.equals(abnormalType, that.abnormalType)
                && Objects.equals(abnormalLevel, that.abnormalLevel)
                && Objects.equals(effectPower, that.effectPower)
                && Objects.equals(statModifiers, that.statModifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, kind, params, abnormalType, abnormalLevel, effectPower, statModifiers);
    }

    @Override
    public String toString() {
        return "SkillEffect[name=" + name + ", kind=" + kind + ", params=" + params + "]";
    }

    public static final class Builder {
        private String name;
        private @Nullable String kind;
        private @Nullable Map<String, String> params;
        private @Nullable String abnormalType;
        private @Nullable Integer abnormalLevel;
        private @Nullable Double effectPower;
        private @Nullable List<SkillStatModifier> statModifiers;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder kind(@Nullable String kind) {
            this.kind = kind;
            return this;
        }

        public Builder params(@Nullable Map<String, String> params) {
            this.params = params;
            return this;
        }

        public Builder abnormalType(@Nullable String abnormalType) {
            this.abnormalType = abnormalType;
            return this;
        }

        public Builder abnormalLevel(@Nullable Integer abnormalLevel) {
            this.abnormalLevel = abnormalLevel;
            return this;
        }

        public Builder effectPower(@Nullable Double effectPower) {
            this.effectPower = effectPower;
            return this;
        }

        public Builder statModifiers(@Nullable List<SkillStatModifier> statModifiers) {
            this.statModifiers = statModifiers;
            return this;
        }

        public SkillEffect build() {
            return new SkillEffect(name, kind, params, abnormalType, abnormalLevel, effectPower, statModifiers);
        }
    }
}
