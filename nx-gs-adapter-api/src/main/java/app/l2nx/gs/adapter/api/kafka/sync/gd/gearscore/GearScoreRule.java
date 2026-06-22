package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One rule inside a {@link GearScoreRuleGroup} — the atomic "this is worth N gear
 * score" statement the wiki renders as a table row. A rule carries a scalar
 * ({@link #getValue() value} with a {@link #getUnit() unit} and optional
 * {@link #getCap() cap}) and/or a {@link #getScaling() scaling} table; either may be
 * present depending on the rule's shape.
 *
 * <p>{@link #getKey() key} is the domain key the host assigns (e.g.
 * {@code WEAPON_PER_POINT}, {@code PROFILE:WEAPON}, {@code OPTION:25002}) — opaque to
 * the platform, used to correlate the rule with per-entity references.</p>
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

    /**
     * Host-assigned domain key identifying the rule (opaque to the platform).
     */
    public String getKey() {
        return key;
    }

    /**
     * Human-readable label for the wiki; {@code null} when none supplied.
     */
    public @Nullable LocalizedText getLabel() {
        return label;
    }

    /**
     * Scalar rate / percentage / flat amount; {@code null} for a purely
     * {@link #getScaling() scaling}-table rule.
     */
    public @Nullable Double getValue() {
        return value;
    }

    /**
     * Unit of {@link #getValue() value} — closed {@code UPPER_SNAKE_CASE} vocabulary
     * ({@code PER_POINT} / {@code PERCENT} / {@code FLAT} / {@code PER_LEVEL} /
     * {@code PER_STEP}); {@code null} when no scalar value is present.
     */
    public @Nullable String getUnit() {
        return unit;
    }

    /**
     * Optional ceiling on the rule's accumulated gear score; {@code null} when
     * uncapped.
     */
    public @Nullable Double getCap() {
        return cap;
    }

    /**
     * Range / step scaling table; {@code null} for a purely scalar rule.
     */
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
