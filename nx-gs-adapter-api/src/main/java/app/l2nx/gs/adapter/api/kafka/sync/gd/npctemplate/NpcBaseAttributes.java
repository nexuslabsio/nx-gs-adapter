package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Base attribute block of an {@link NpcTemplate} — the six primary stats. Carried
 * as a nested object; all fields {@link Nullable}.
 *
 * <p>The INT attribute is named {@code intel} because {@code int} is a Java
 * reserved word; on the wire and in storage it remains the "INT" attribute.</p>
 */
public final class NpcBaseAttributes {

    private final @Nullable Integer str;
    private final @Nullable Integer dex;
    private final @Nullable Integer con;
    private final @Nullable Integer intel;
    private final @Nullable Integer wit;
    private final @Nullable Integer men;

    public NpcBaseAttributes(@Nullable Integer str,
                             @Nullable Integer dex,
                             @Nullable Integer con,
                             @Nullable Integer intel,
                             @Nullable Integer wit,
                             @Nullable Integer men) {
        this.str = str;
        this.dex = dex;
        this.con = con;
        this.intel = intel;
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

    public @Nullable Integer getIntel() {
        return intel;
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
                .intel(intel)
                .wit(wit)
                .men(men);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcBaseAttributes)) return false;
        NpcBaseAttributes that = (NpcBaseAttributes) o;
        return Objects.equals(str, that.str)
                && Objects.equals(dex, that.dex)
                && Objects.equals(con, that.con)
                && Objects.equals(intel, that.intel)
                && Objects.equals(wit, that.wit)
                && Objects.equals(men, that.men);
    }

    @Override
    public int hashCode() {
        return Objects.hash(str, dex, con, intel, wit, men);
    }

    @Override
    public String toString() {
        return "NpcBaseAttributes[str=" + str + ", dex=" + dex + ", con=" + con
                + ", intel=" + intel + ", wit=" + wit + ", men=" + men + "]";
    }

    public static final class Builder {
        private @Nullable Integer str;
        private @Nullable Integer dex;
        private @Nullable Integer con;
        private @Nullable Integer intel;
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

        public Builder intel(@Nullable Integer intel) {
            this.intel = intel;
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

        public NpcBaseAttributes build() {
            return new NpcBaseAttributes(str, dex, con, intel, wit, men);
        }
    }
}
