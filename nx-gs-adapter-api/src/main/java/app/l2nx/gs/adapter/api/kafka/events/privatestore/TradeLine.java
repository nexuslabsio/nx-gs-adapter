package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.domain.item.ItemAttribute;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * One transferred position in a closed {@link PrivateStorePurchaseEvent}.
 * Identity tuple {@code (itemId, enchantLevel, attributes)} lets consumers
 * pivot at any granularity.
 */
public final class TradeLine {

    private final long itemId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<ItemAttribute, Integer> attributes;
    private final long count;
    private final long unitPrice;
    private final long currencyItemId;

    public TradeLine(long itemId,
                     @Nullable Integer enchantLevel,
                     @Nullable Map<ItemAttribute, Integer> attributes,
                     long count,
                     long unitPrice,
                     long currencyItemId) {
        this.itemId = itemId;
        this.enchantLevel = enchantLevel;
        this.attributes = freezeMap(attributes);
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
    }

    /**
     * Source-side L2 item template ID.
     */
    public long getItemId() {
        return itemId;
    }

    /**
     * Enchant level. {@code null} when the item type has no enchant concept;
     * {@code 0} for enchantable-but-unenchanted; {@code > 0} otherwise.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public Map<ItemAttribute, Integer> getAttributes() {
        return attributes == null ? Collections.emptyMap() : attributes;
    }

    public long getCount() {
        return count;
    }

    public long getUnitPrice() {
        return unitPrice;
    }

    /**
     * Currency item id (typically {@code 57} = Adena; can differ for
     * alt-currency stores).
     */
    public long getCurrencyItemId() {
        return currencyItemId;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .count(count)
                .unitPrice(unitPrice)
                .currencyItemId(currencyItemId);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static @Nullable Map<ItemAttribute, Integer> freezeMap(@Nullable Map<ItemAttribute, Integer> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new EnumMap<ItemAttribute, Integer>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TradeLine)) return false;
        TradeLine that = (TradeLine) o;
        return itemId == that.itemId
                && count == that.count
                && unitPrice == that.unitPrice
                && currencyItemId == that.currencyItemId
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, enchantLevel, attributes, count, unitPrice, currencyItemId);
    }

    @Override
    public String toString() {
        return "TradeLine[itemId=" + itemId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemId=" + currencyItemId + "]";
    }

    public static final class Builder {
        private long itemId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<ItemAttribute, Integer> attributes;
        private long count;
        private long unitPrice;
        private long currencyItemId;

        public Builder itemId(long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder attributes(@Nullable Map<ItemAttribute, Integer> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder count(long count) {
            this.count = count;
            return this;
        }

        public Builder unitPrice(long unitPrice) {
            this.unitPrice = unitPrice;
            return this;
        }

        public Builder currencyItemId(long currencyItemId) {
            this.currencyItemId = currencyItemId;
            return this;
        }

        public TradeLine build() {
            return new TradeLine(itemId, enchantLevel, attributes, count, unitPrice, currencyItemId);
        }
    }
}
