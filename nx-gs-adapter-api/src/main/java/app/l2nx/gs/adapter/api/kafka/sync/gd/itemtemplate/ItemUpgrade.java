package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Enchant / attribute / crystallize mechanics; a field is {@code null} when the build lacks the mechanic
 * (e.g. no attribute system pre-Gracia).
 */
public final class ItemUpgrade {

    private final @Nullable Boolean enchantable;
    private final @Nullable Integer defaultEnchantLevel;
    private final @Nullable Boolean attributable;
    private final @Nullable Boolean crystallizable;
    private final @Nullable Integer crystalCount;

    public ItemUpgrade(
            @Nullable Boolean enchantable,
            @Nullable Integer defaultEnchantLevel,
            @Nullable Boolean attributable,
            @Nullable Boolean crystallizable,
            @Nullable Integer crystalCount) {
        this.enchantable = enchantable;
        this.defaultEnchantLevel = defaultEnchantLevel;
        this.attributable = attributable;
        this.crystallizable = crystallizable;
        this.crystalCount = crystalCount;
    }

    /**
     * Capability, not a level: max enchant is global server config.
     */
    public @Nullable Boolean getEnchantable() {
        return enchantable;
    }

    public @Nullable Integer getDefaultEnchantLevel() {
        return defaultEnchantLevel;
    }

    public @Nullable Boolean getAttributable() {
        return attributable;
    }

    public @Nullable Boolean getCrystallizable() {
        return crystallizable;
    }

    public @Nullable Integer getCrystalCount() {
        return crystalCount;
    }

    public Builder toBuilder() {
        return new Builder()
                .enchantable(enchantable)
                .defaultEnchantLevel(defaultEnchantLevel)
                .attributable(attributable)
                .crystallizable(crystallizable)
                .crystalCount(crystalCount);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemUpgrade)) return false;
        ItemUpgrade that = (ItemUpgrade) o;
        return Objects.equals(enchantable, that.enchantable)
                && Objects.equals(defaultEnchantLevel, that.defaultEnchantLevel)
                && Objects.equals(attributable, that.attributable)
                && Objects.equals(crystallizable, that.crystallizable)
                && Objects.equals(crystalCount, that.crystalCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enchantable, defaultEnchantLevel, attributable, crystallizable, crystalCount);
    }

    @Override
    public String toString() {
        return "ItemUpgrade[enchantable=" + enchantable + ", attributable=" + attributable + ", crystalCount="
                + crystalCount + "]";
    }

    public static final class Builder {
        private @Nullable Boolean enchantable;
        private @Nullable Integer defaultEnchantLevel;
        private @Nullable Boolean attributable;
        private @Nullable Boolean crystallizable;
        private @Nullable Integer crystalCount;

        public Builder enchantable(@Nullable Boolean enchantable) {
            this.enchantable = enchantable;
            return this;
        }

        public Builder defaultEnchantLevel(@Nullable Integer defaultEnchantLevel) {
            this.defaultEnchantLevel = defaultEnchantLevel;
            return this;
        }

        public Builder attributable(@Nullable Boolean attributable) {
            this.attributable = attributable;
            return this;
        }

        public Builder crystallizable(@Nullable Boolean crystallizable) {
            this.crystallizable = crystallizable;
            return this;
        }

        public Builder crystalCount(@Nullable Integer crystalCount) {
            this.crystalCount = crystalCount;
            return this;
        }

        public ItemUpgrade build() {
            return new ItemUpgrade(enchantable, defaultEnchantLevel, attributable, crystallizable, crystalCount);
        }
    }
}
