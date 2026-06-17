package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One row of a gear-score scaling table on a {@link GearScoreRule} — a half-open
 * range {@code [from, to]} mapping to a flat {@link #getValue() value}. Used for
 * stepped / range-based bonuses (enchant brackets, aura levels, …).
 *
 * <p>{@link #getTo() to} is {@code null} for the final, open-ended range (covers
 * every value at or above {@link #getFrom() from}).</p>
 */
public final class GearScoreScalingStep {

    private final int from;
    private final @Nullable Integer to;
    private final double value;

    public GearScoreScalingStep(int from,
                                @Nullable Integer to,
                                double value) {
        this.from = from;
        this.to = to;
        this.value = value;
    }

    /**
     * Inclusive lower bound of the range.
     */
    public int getFrom() {
        return from;
    }

    /**
     * Inclusive upper bound of the range; {@code null} for an open-ended top range.
     */
    public @Nullable Integer getTo() {
        return to;
    }

    /**
     * Gear-score value applied across this range.
     */
    public double getValue() {
        return value;
    }

    public Builder toBuilder() {
        return new Builder()
                .from(from)
                .to(to)
                .value(value);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GearScoreScalingStep)) return false;
        GearScoreScalingStep that = (GearScoreScalingStep) o;
        return from == that.from
                && Double.compare(value, that.value) == 0
                && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to, value);
    }

    @Override
    public String toString() {
        return "GearScoreScalingStep[from=" + from
                + ", to=" + to
                + ", value=" + value + "]";
    }

    public static final class Builder {
        private int from;
        private @Nullable Integer to;
        private double value;

        public Builder from(int from) {
            this.from = from;
            return this;
        }

        public Builder to(@Nullable Integer to) {
            this.to = to;
            return this;
        }

        public Builder value(double value) {
            this.value = value;
            return this;
        }

        public GearScoreScalingStep build() {
            return new GearScoreScalingStep(from, to, value);
        }
    }
}
