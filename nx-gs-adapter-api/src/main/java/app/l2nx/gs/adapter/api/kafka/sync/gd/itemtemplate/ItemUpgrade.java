package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Upgrade / modification mechanics of an {@link ItemTemplate} — "how this item can
 * be improved or converted": enchanting, attribute (element) infusion, and
 * crystallization. Grouped so the upgrade surface is one cohesive object.
 *
 * <p>All fields {@link Nullable}; a build that lacks a mechanic (no attribute
 * system pre-Gracia, no grade/crystal system) leaves the corresponding field
 * {@code null}. All values are server-side / memory-sourced (Phase 1).</p>
 */
public final class ItemUpgrade {

    private final @Nullable Boolean enchantable;
    private final @Nullable Integer defaultEnchantLevel;
    private final @Nullable Boolean attributable;
    private final @Nullable Boolean crystallizable;
    private final @Nullable Integer crystalCount;

    private ItemUpgrade(Builder b) {
        this.enchantable = b.enchantable;
        this.defaultEnchantLevel = b.defaultEnchantLevel;
        this.attributable = b.attributable;
        this.crystallizable = b.crystallizable;
        this.crystalCount = b.crystalCount;
    }

    /**
     * Whether the item may be enchanted at all (server {@code enchant_enabled} flag);
     * {@code null} = unknown. The datapack carries no per-item max enchant level — that
     * is a global server config — so this is a boolean capability, not a level.
     */
    public @Nullable Boolean getEnchantable() {
        return enchantable;
    }

    /**
     * Pre-enchanted base level at creation; {@code null}/0 = none.
     */
    public @Nullable Integer getDefaultEnchantLevel() {
        return defaultEnchantLevel;
    }

    /**
     * Can receive an attribute (element); business vocabulary: element → attribute.
     */
    public @Nullable Boolean getAttributable() {
        return attributable;
    }

    /**
     * Can be crystallized (≠ {@link #getCrystalCount()}).
     */
    public @Nullable Boolean getCrystallizable() {
        return crystallizable;
    }

    /**
     * Number of crystals produced on crystallization.
     */
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
        return "ItemUpgrade[enchantable=" + enchantable + ", attributable=" + attributable
                + ", crystalCount=" + crystalCount + "]";
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
            return new ItemUpgrade(this);
        }
    }
}
