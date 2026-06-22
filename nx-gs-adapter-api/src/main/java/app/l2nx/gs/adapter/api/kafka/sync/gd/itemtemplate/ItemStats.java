package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import app.l2nx.gs.adapter.api.domain.stat.Stat;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Stats block of an {@link ItemTemplate} — the gameplay numbers that make a weapon a
 * weapon / armor armor. Carried as a nested object so etc-items (no stat profile) simply
 * omit it ({@code stats == null} on this object, and the object itself may be omitted on
 * the template).
 *
 * <p>{@link #getStats()} is the single home for every stat value — combat numbers,
 * elemental power/resist, base-stat bonuses, and weapon mechanics (soulshot/spiritshot
 * count, MP-per-attack, random damage, attack range) — keyed by the canonical
 * {@link Stat} token name. {@code magicWeapon} is the only stat datum kept out of the
 * map (it is a boolean, not a magnitude).</p>
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
     * Every stat the item carries, keyed by the canonical {@link Stat} token name
     * (e.g. {@code P_ATK}, {@code MAX_HP}, {@code FIRE_RES}, {@code SOULSHOT_COUNT}).
     * {@code null} when the item carries no stats. The producer maps its build-specific
     * stat representation onto {@link Stat} tokens; unmappable stats are dropped so
     * the key set stays within the closed vocabulary.
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
