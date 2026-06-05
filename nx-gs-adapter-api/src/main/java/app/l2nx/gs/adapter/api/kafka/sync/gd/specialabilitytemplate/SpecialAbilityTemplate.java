package app.l2nx.gs.adapter.api.kafka.sync.gd.specialabilitytemplate;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Build-agnostic special-ability (ensoul) wire DTO — the common L2 denominator for one
 * soul-crystal stone, carried as the payload of {@code GameDataSyncEvent} on the {@code gd}
 * (game-data) sync stream's {@code specialabilitytemplate} entity topic. Each host build
 * supplies its own provider that maps its core's internal ensoul representation into this
 * shape; nothing here names a specific core.
 *
 * <p>One {@code SpecialAbilityTemplate} is the whole aggregate for a stone item id, kept
 * self-contained: the options it offers ({@link #getOptions()}, each granting a skill) plus
 * the grade-mediated add/change/remove prices ({@link #getPrices()}). Stone↔weapon
 * compatibility is by crystal grade ({@link #getCrystalType()}), not an explicit
 * per-weapon list. The consumer upserts the parent and replaces its children atomically.</p>
 *
 * <p><b>Nullability:</b> only {@link #getId()} (the stone item id) is non-null.
 * {@code slotType} is the core's raw slot int (not mapped to a token). Item references
 * inside prices use the canonical {@code itemTemplateId} name; option skills use
 * {@code skillTemplateId}.</p>
 */
public final class SpecialAbilityTemplate {

    private final int id;
    private final @Nullable Integer slotType;
    private final @Nullable String crystalType;
    private final @Nullable List<SpecialAbilityOption> options;
    private final @Nullable List<SpecialAbilityPrice> prices;

    public SpecialAbilityTemplate(int id,
                                  @Nullable Integer slotType,
                                  @Nullable String crystalType,
                                  @Nullable List<SpecialAbilityOption> options,
                                  @Nullable List<SpecialAbilityPrice> prices) {
        this.id = id;
        this.slotType = slotType;
        this.crystalType = crystalType;
        this.options = options == null ? null
                : Collections.unmodifiableList(new ArrayList<SpecialAbilityOption>(options));
        this.prices = prices == null ? null
                : Collections.unmodifiableList(new ArrayList<SpecialAbilityPrice>(prices));
    }

    public int getId() {
        return id;
    }

    /**
     * Raw core slot type of the stone (e.g. main vs additional slot); the wire keeps the
     * core int rather than fabricating a token mapping.
     */
    public @Nullable Integer getSlotType() {
        return slotType;
    }

    /**
     * Crystal grade of the stone in canonical UPPER_SNAKE form ({@code S84}, {@code S80},
     * {@code S}, …); the grade is what matches a stone to a weapon and to its prices.
     */
    public @Nullable String getCrystalType() {
        return crystalType;
    }

    /**
     * Special-ability options this stone offers, each granting a skill; {@code null} if none.
     */
    public @Nullable List<SpecialAbilityOption> getOptions() {
        return options;
    }

    /**
     * Gemstone/adena prices to add/change/remove an ability with this stone's grade;
     * {@code null} if none supplied.
     */
    public @Nullable List<SpecialAbilityPrice> getPrices() {
        return prices;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .slotType(slotType)
                .crystalType(crystalType)
                .options(options)
                .prices(prices);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpecialAbilityTemplate)) return false;
        SpecialAbilityTemplate that = (SpecialAbilityTemplate) o;
        return id == that.id
                && Objects.equals(slotType, that.slotType)
                && Objects.equals(crystalType, that.crystalType)
                && Objects.equals(options, that.options)
                && Objects.equals(prices, that.prices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, slotType, crystalType, options, prices);
    }

    @Override
    public String toString() {
        return "SpecialAbilityTemplate[id=" + id + ", crystalType=" + crystalType + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable Integer slotType;
        private @Nullable String crystalType;
        private @Nullable List<SpecialAbilityOption> options;
        private @Nullable List<SpecialAbilityPrice> prices;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder slotType(@Nullable Integer slotType) {
            this.slotType = slotType;
            return this;
        }

        public Builder crystalType(@Nullable String crystalType) {
            this.crystalType = crystalType;
            return this;
        }

        public Builder options(@Nullable List<SpecialAbilityOption> options) {
            this.options = options;
            return this;
        }

        public Builder prices(@Nullable List<SpecialAbilityPrice> prices) {
            this.prices = prices;
            return this;
        }

        public SpecialAbilityTemplate build() {
            return new SpecialAbilityTemplate(id, slotType, crystalType, options, prices);
        }
    }
}
