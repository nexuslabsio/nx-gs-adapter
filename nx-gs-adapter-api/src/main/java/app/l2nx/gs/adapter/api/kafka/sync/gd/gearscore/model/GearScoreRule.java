package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.model;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * {@code key} is the host-assigned opaque domain key (e.g. {@code WEAPON_PER_POINT}, {@code PROFILE:WEAPON},
 * {@code OPTION:25002}) that correlates the rule with per-entity references. Carries a scalar value and/or a scaling table.
 */
public final class GearScoreRule {

    private final String key;
    private final @Nullable LocalizedText label;
    private final @Nullable Double value;
    private final @Nullable String unit;
    private final @Nullable Double cap;
    private final @Nullable List<GearScoreScalingStep> scaling;

    public GearScoreRule(
            String key,
            @Nullable LocalizedText label,
            @Nullable Double value,
            @Nullable String unit,
            @Nullable Double cap,
            @Nullable List<GearScoreScalingStep> scaling) {
        this.key = Objects.requireNonNull(key, "GearScoreRule.key is required");
        this.label = label;
        this.value = value;
        this.unit = unit;
        this.cap = cap;
        this.scaling =
                scaling == null ? null : Collections.unmodifiableList(new ArrayList<GearScoreScalingStep>(scaling));
    }

    public String getKey() {
        return key;
    }

    public @Nullable LocalizedText getLabel() {
        return label;
    }

    public @Nullable Double getValue() {
        return value;
    }

    /**
     * Closed vocabulary: {@code PER_POINT} / {@code PERCENT} / {@code FLAT} / {@code PER_LEVEL} / {@code PER_STEP}.
     */
    public @Nullable String getUnit() {
        return unit;
    }

    /**
     * {@code null} = uncapped.
     */
    public @Nullable Double getCap() {
        return cap;
    }

    public @Nullable List<GearScoreScalingStep> getScaling() {
        return scaling;
    }

    public Builder toBuilder() {
        return new Builder()
                .key(key)
                .label(label)
                .value(value)
                .unit(unit)
                .cap(cap)
                .scaling(scaling);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GearScoreRule)) return false;
        GearScoreRule that = (GearScoreRule) o;
        return key.equals(that.key)
                && Objects.equals(label, that.label)
                && Objects.equals(value, that.value)
                && Objects.equals(unit, that.unit)
                && Objects.equals(cap, that.cap)
                && Objects.equals(scaling, that.scaling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, label, value, unit, cap, scaling);
    }

    @Override
    public String toString() {
        return "GearScoreRule[key=" + key + ", value=" + value + ", unit=" + unit + "]";
    }

    public static final class Builder {
        private @Nullable String key;
        private @Nullable LocalizedText label;
        private @Nullable Double value;
        private @Nullable String unit;
        private @Nullable Double cap;
        private @Nullable List<GearScoreScalingStep> scaling;

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder label(@Nullable LocalizedText label) {
            this.label = label;
            return this;
        }

        public Builder value(@Nullable Double value) {
            this.value = value;
            return this;
        }

        public Builder unit(@Nullable String unit) {
            this.unit = unit;
            return this;
        }

        public Builder cap(@Nullable Double cap) {
            this.cap = cap;
            return this;
        }

        public Builder scaling(@Nullable List<GearScoreScalingStep> scaling) {
            this.scaling = scaling;
            return this;
        }

        public GearScoreRule build() {
            return new GearScoreRule(key, label, value, unit, cap, scaling);
        }
    }
}
