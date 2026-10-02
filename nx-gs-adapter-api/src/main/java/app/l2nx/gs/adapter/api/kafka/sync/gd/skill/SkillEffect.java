package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One effect a {@link SkillLevel} applies. {@code name} (non-null) is the effect handler name, e.g.
 * {@code p_attack}, {@code Stun}; {@code params} is its flat string-to-string parameter map.
 * {@code kind} is {@code TARGET} / {@code SELF} / {@code PASSIVE} (while active); {@code null} means {@code TARGET}.
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

    public @Nullable String getKind() {
        return kind;
    }

    public @Nullable Map<String, String> getParams() {
        return params;
    }

    /** Buff-slot stacking type; {@code null} when the effect occupies no buff slot. */
    public @Nullable String getAbnormalType() {
        return abnormalType;
    }

    /** Higher overwrites lower within {@code abnormalType}. */
    public @Nullable Integer getAbnormalLevel() {
        return abnormalLevel;
    }

    /** Effect's own magnitude operand (land-rate / value base), distinct from the skill-level {@code power}. */
    public @Nullable Double getEffectPower() {
        return effectPower;
    }

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
