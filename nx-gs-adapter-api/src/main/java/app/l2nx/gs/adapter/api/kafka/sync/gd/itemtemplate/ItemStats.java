package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import app.l2nx.gs.adapter.api.domain.stat.Stat;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Every stat value lives in {@link #getStats()} keyed by {@link Stat} token; {@code magicWeapon} stays out of it
 * as a boolean, not a magnitude.
 */
public final class ItemStats {

    private final @Nullable Boolean magicWeapon;
    private final @Nullable Map<String, Double> stats;

    public ItemStats(@Nullable Boolean magicWeapon, @Nullable Map<String, Double> stats) {
        this.magicWeapon = magicWeapon;
        this.stats = stats;
    }

    public @Nullable Boolean getMagicWeapon() {
        return magicWeapon;
    }

    /**
     * Unmappable source stats are dropped so keys stay within the closed vocabulary.
     */
    public @Nullable Map<String, Double> getStats() {
        return stats;
    }

    public Builder toBuilder() {
        return new Builder().magicWeapon(magicWeapon).stats(stats);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemStats)) return false;
        ItemStats that = (ItemStats) o;
        return Objects.equals(magicWeapon, that.magicWeapon) && Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(magicWeapon, stats);
    }

    @Override
    public String toString() {
        return "ItemStats[magicWeapon=" + magicWeapon + ", stats=" + stats + "]";
    }

    public static final class Builder {
        private @Nullable Boolean magicWeapon;
        private @Nullable Map<String, Double> stats;

        public Builder magicWeapon(@Nullable Boolean magicWeapon) {
            this.magicWeapon = magicWeapon;
            return this;
        }

        public Builder stats(@Nullable Map<String, Double> stats) {
            this.stats = stats;
            return this;
        }

        public ItemStats build() {
            return new ItemStats(magicWeapon, stats);
        }
    }
}
