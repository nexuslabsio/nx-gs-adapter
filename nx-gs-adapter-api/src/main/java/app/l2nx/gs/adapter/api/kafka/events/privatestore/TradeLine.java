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
 *
 * <p><b>Rename in flight (spec 065 §2.2, release N of 2).</b> {@code itemId}
 * and {@code currencyItemId} are TEMPLATE references and are being renamed to
 * {@code itemTemplateId} / {@code currencyItemTemplateId}. All four fields
 * ride the wire this release — producers set each deprecated field and its
 * replacement to the same value, consumers should read the new names with a
 * fallback to the old ones. The deprecated fields are removed once every
 * producer emits the new names (release N+1).</p>
 */
public final class TradeLine {

    private final long itemId;
    private final @Nullable Long itemTemplateId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<Attribute, Integer> attributes;
    private final long count;
    private final long unitPrice;
    private final long currencyItemId;
    private final @Nullable Long currencyItemTemplateId;

    public TradeLine(
            long itemId,
            @Nullable Long itemTemplateId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPrice,
            long currencyItemId,
            @Nullable Long currencyItemTemplateId) {
        this.itemId = itemId;
        this.itemTemplateId = itemTemplateId;
        this.enchantLevel = enchantLevel;
        this.attributes = freezeMap(attributes);
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
        this.currencyItemTemplateId = currencyItemTemplateId;
    }

    /**
     * @deprecated renamed to {@link #getItemTemplateId()} — the field is a
     *     TEMPLATE id, not an instance id. Removed once every producer emits
     *     {@code itemTemplateId} (bohpts game-server restart under the new
     *     adapter jar).
     */
    @Deprecated
    public long getItemId() {
        return itemId;
    }

    /**
     * Source-side L2 item template ID. {@code null} on old producers that
     * only emit the deprecated {@link #getItemId() itemId}.
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
     * @deprecated renamed to {@link #getCurrencyItemTemplateId()} — the field
     *     is a TEMPLATE id, not an instance id. Removed once every producer
     *     emits {@code currencyItemTemplateId} (bohpts game-server restart
     *     under the new adapter jar).
     */
    @Deprecated
    public long getCurrencyItemId() {
        return currencyItemId;
    }

    /**
     * Currency item template id (typically {@code 57} = Adena; can differ for
     * alt-currency stores). {@code null} on old producers that only emit the
     * deprecated {@link #getCurrencyItemId() currencyItemId}.
     */
    public @Nullable Long getCurrencyItemTemplateId() {
        return currencyItemTemplateId;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .itemTemplateId(itemTemplateId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .count(count)
                .unitPrice(unitPrice)
                .currencyItemId(currencyItemId)
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
        return itemId == that.itemId
                && count == that.count
                && unitPrice == that.unitPrice
                && currencyItemId == that.currencyItemId
                && Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(currencyItemTemplateId, that.currencyItemTemplateId)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                itemId,
                itemTemplateId,
                enchantLevel,
                attributes,
                count,
                unitPrice,
                currencyItemId,
                currencyItemTemplateId);
    }

    @Override
    public String toString() {
        return "TradeLine[itemId=" + itemId
                + ", itemTemplateId=" + itemTemplateId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemId=" + currencyItemId
                + ", currencyItemTemplateId=" + currencyItemTemplateId + "]";
    }

    public static final class Builder {
        private long itemId;
        private @Nullable Long itemTemplateId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;
        private long count;
        private long unitPrice;
        private long currencyItemId;
        private @Nullable Long currencyItemTemplateId;

        /**
         * @deprecated renamed to {@link #itemTemplateId(long)}.
         */
        @Deprecated
        public Builder itemId(long itemId) {
            this.itemId = itemId;
            return this;
        }

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

        /**
         * @deprecated renamed to {@link #currencyItemTemplateId(long)}.
         */
        @Deprecated
        public Builder currencyItemId(long currencyItemId) {
            this.currencyItemId = currencyItemId;
            return this;
        }

        public Builder currencyItemTemplateId(@Nullable Long currencyItemTemplateId) {
            this.currencyItemTemplateId = currencyItemTemplateId;
            return this;
        }

        public TradeLine build() {
            return new TradeLine(
                    itemId,
                    itemTemplateId,
                    enchantLevel,
                    attributes,
                    count,
                    unitPrice,
                    currencyItemId,
                    currencyItemTemplateId);
        }
    }
}
