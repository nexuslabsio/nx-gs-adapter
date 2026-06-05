package app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Cohesive cluster of an {@link ArmorSetTemplate}'s flat base-stat bonuses — the STR / DEX /
 * CON / INT / WIT / MEN deltas applied while the full set is worn. Grouped (the same way
 * {@code SkillFlags} groups a skill's classification flags) so the header surface is one
 * object instead of a scatter of integers; the consumer unwraps it flat into columns.
 *
 * <p>All fields are {@link Nullable Integer} and <b>may be negative</b> (some sets trade one
 * stat for another). The {@code int} stat is named {@code intBonus} because {@code int} is a
 * Java keyword; DB column {@code int_bonus}.</p>
 */
public final class ArmorSetStatBonus {

    private final @Nullable Integer str;
    private final @Nullable Integer dex;
    private final @Nullable Integer con;
    private final @Nullable Integer intBonus;
    private final @Nullable Integer wit;
    private final @Nullable Integer men;

    public ArmorSetStatBonus(@Nullable Integer str,
                             @Nullable Integer dex,
                             @Nullable Integer con,
                             @Nullable Integer intBonus,
                             @Nullable Integer wit,
                             @Nullable Integer men) {
        this.str = str;
        this.dex = dex;
        this.con = con;
        this.intBonus = intBonus;
        this.wit = wit;
        this.men = men;
    }

    public @Nullable Integer getStr() {
        return str;
    }

    public @Nullable Integer getDex() {
        return dex;
    }

    public @Nullable Integer getCon() {
        return con;
    }

    /**
     * INT bonus. Field/accessor is {@code intBonus}/{@code getIntBonus} ({@code int} is a
     * Java keyword); DB column {@code int_bonus}.
     */
    public @Nullable Integer getIntBonus() {
        return intBonus;
    }

    public @Nullable Integer getWit() {
        return wit;
    }

    public @Nullable Integer getMen() {
        return men;
    }

    public Builder toBuilder() {
        return new Builder()
                .str(str)
                .dex(dex)
                .con(con)
                .intBonus(intBonus)
                .wit(wit)
                .men(men);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArmorSetStatBonus)) return false;
        ArmorSetStatBonus that = (ArmorSetStatBonus) o;
        return Objects.equals(str, that.str)
                && Objects.equals(dex, that.dex)
                && Objects.equals(con, that.con)
                && Objects.equals(intBonus, that.intBonus)
                && Objects.equals(wit, that.wit)
                && Objects.equals(men, that.men);
    }

    @Override
    public int hashCode() {
        return Objects.hash(str, dex, con, intBonus, wit, men);
    }

    @Override
    public String toString() {
        return "ArmorSetStatBonus[str=" + str + ", dex=" + dex + ", con=" + con
                + ", intBonus=" + intBonus + ", wit=" + wit + ", men=" + men + "]";
    }

    public static final class Builder {
        private @Nullable Integer str;
        private @Nullable Integer dex;
        private @Nullable Integer con;
        private @Nullable Integer intBonus;
        private @Nullable Integer wit;
        private @Nullable Integer men;

        public Builder str(@Nullable Integer str) {
            this.str = str;
            return this;
        }

        public Builder dex(@Nullable Integer dex) {
            this.dex = dex;
            return this;
        }

        public Builder con(@Nullable Integer con) {
            this.con = con;
            return this;
        }

        public Builder intBonus(@Nullable Integer intBonus) {
            this.intBonus = intBonus;
            return this;
        }

        public Builder wit(@Nullable Integer wit) {
            this.wit = wit;
            return this;
        }

        public Builder men(@Nullable Integer men) {
            this.men = men;
            return this;
        }

        public ArmorSetStatBonus build() {
            return new ArmorSetStatBonus(str, dex, con, intBonus, wit, men);
        }
    }
}
