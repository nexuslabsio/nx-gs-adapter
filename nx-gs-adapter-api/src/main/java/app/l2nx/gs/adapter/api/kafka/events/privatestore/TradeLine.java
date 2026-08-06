package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One transferred position in a closed {@link PrivateStorePurchaseEvent}.
 * Identity tuple {@code (itemTemplateId, enchantLevel, attributes)} lets
 * consumers pivot at any granularity.
 */
public final class TradeLine {

    private final @Nullable Long itemTemplateId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<Attribute, Integer> attributes;
    private final long count;
    private final long unitPrice;
    private final @Nullable Long currencyItemTemplateId;

    public TradeLine(
            @Nullable Long itemTemplateId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPrice,
            @Nullable Long currencyItemTemplateId) {
        this.itemTemplateId = itemTemplateId;
        this.enchantLevel = enchantLevel;
        this.attributes = freezeMap(attributes);
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemTemplateId = currencyItemTemplateId;
    }

    /**
     * Source-side L2 item template ID. {@code null} when the host does not
     * report it.
     */
    public @Nullable Long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Enchant level. {@code null} when the item type has no enchant concept;
     * {@code 0} for enchantable-but-unenchanted; {@code > 0} otherwise.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public Map<Attribute, Integer> getAttributes() {
        return attributes == null ? Collections.emptyMap() : attributes;
    }

    public long getCount() {
        return count;
    }

    public long getUnitPrice() {
        return unitPrice;
    }

    /**
     * Currency item template id (typically {@code 57} = Adena; can differ for
     * alt-currency stores). {@code null} when the host does not report it.
     */
    public @Nullable Long getCurrencyItemTemplateId() {
        return currencyItemTemplateId;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemTemplateId(itemTemplateId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .count(count)
                .unitPrice(unitPrice)
                .currencyItemTemplateId(currencyItemTemplateId);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static @Nullable Map<Attribute, Integer> freezeMap(@Nullable Map<Attribute, Integer> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new EnumMap<Attribute, Integer>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TradeLine)) return false;
        TradeLine that = (TradeLine) o;
        return count == that.count
                && unitPrice == that.unitPrice
                && Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(currencyItemTemplateId, that.currencyItemTemplateId)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, enchantLevel, attributes, count, unitPrice, currencyItemTemplateId);
    }

    @Override
    public String toString() {
        return "TradeLine[itemTemplateId=" + itemTemplateId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemTemplateId=" + currencyItemTemplateId + "]";
    }

    public static final class Builder {
        private @Nullable Long itemTemplateId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;
        private long count;
        private long unitPrice;
        private @Nullable Long currencyItemTemplateId;

        public Builder itemTemplateId(@Nullable Long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder attributes(@Nullable Map<Attribute, Integer> attributes) {
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

        public Builder currencyItemTemplateId(@Nullable Long currencyItemTemplateId) {
            this.currencyItemTemplateId = currencyItemTemplateId;
            return this;
        }

        public TradeLine build() {
            return new TradeLine(itemTemplateId, enchantLevel, attributes, count, unitPrice, currencyItemTemplateId);
        }
    }
}
