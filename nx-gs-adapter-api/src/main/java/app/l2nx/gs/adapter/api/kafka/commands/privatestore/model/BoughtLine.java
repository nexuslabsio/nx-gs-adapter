package app.l2nx.gs.adapter.api.kafka.commands.privatestore.model;

import app.l2nx.gs.adapter.api.kafka.commands.privatestore.BuyFromPrivateStoreCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.BuyFromPrivateStoreResult;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Echo of an executed {@link BuyFromPrivateStoreCommand} lot. Purchases are all-or-nothing, so count and price
 * always equal the request.
 */
public final class BoughtLine {

    private final int itemId;
    private final long itemTemplateId;
    private final @Nullable Integer enchantLevel;
    private final long count;
    private final long unitPriceAdena;

    public BoughtLine(
            int itemId, long itemTemplateId, @Nullable Integer enchantLevel, long count, long unitPriceAdena) {
        this.itemId = itemId;
        this.itemTemplateId = itemTemplateId;
        this.enchantLevel = enchantLevel;
        this.count = count;
        this.unitPriceAdena = unitPriceAdena;
    }

    public int getItemId() {
        return itemId;
    }

    public long getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public long getCount() {
        return count;
    }

    /** Excludes the burned surcharge, reported once per deal on {@link BuyFromPrivateStoreResult#getTaxAdena()}. */
    public long getUnitPriceAdena() {
        return unitPriceAdena;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .itemTemplateId(itemTemplateId)
                .enchantLevel(enchantLevel)
                .count(count)
                .unitPriceAdena(unitPriceAdena);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BoughtLine)) return false;
        BoughtLine that = (BoughtLine) o;
        return itemId == that.itemId
                && itemTemplateId == that.itemTemplateId
                && count == that.count
                && unitPriceAdena == that.unitPriceAdena
                && Objects.equals(enchantLevel, that.enchantLevel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, itemTemplateId, enchantLevel, count, unitPriceAdena);
    }

    @Override
    public String toString() {
        return "BoughtLine[itemId=" + itemId
                + ", itemTemplateId=" + itemTemplateId
                + ", enchantLevel=" + enchantLevel
                + ", count=" + count
                + ", unitPriceAdena=" + unitPriceAdena + "]";
    }

    public static final class Builder {
        private int itemId;
        private long itemTemplateId;
        private @Nullable Integer enchantLevel;
        private long count;
        private long unitPriceAdena;

        public Builder itemId(int itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder itemTemplateId(long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder count(long count) {
            this.count = count;
            return this;
        }

        public Builder unitPriceAdena(long unitPriceAdena) {
            this.unitPriceAdena = unitPriceAdena;
            return this;
        }

        public BoughtLine build() {
            return new BoughtLine(itemId, itemTemplateId, enchantLevel, count, unitPriceAdena);
        }
    }
}
