package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One executed lot of a {@link BuyFromPrivateStoreCommand} — what the buyer
 * actually received, echoed back from the lot the host resolved.
 *
 * <p>Because purchases are all-or-nothing, {@link #getCount() count} always
 * equals the requested count and {@link #getUnitPriceAdena() unitPriceAdena}
 * the requested price; the line is echoed so the caller can render a receipt
 * (and persist an audit row) without re-reading its own request.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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

    /**
     * Object id of the specific item instance the buyer saw in the market
     * book — the same identity key as the requesting {@link BuyLine#getItemId()}.
     */
    public int getItemId() {
        return itemId;
    }

    public long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Enchant level of the received item. {@code null} for templates that
     * cannot be enchanted.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public long getCount() {
        return count;
    }

    /**
     * Per-unit adena price paid to the seller — the burned surcharge is NOT
     * included here, it is reported once per deal on
     * {@link BuyFromPrivateStoreResult#getTaxAdena()}.
     */
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
