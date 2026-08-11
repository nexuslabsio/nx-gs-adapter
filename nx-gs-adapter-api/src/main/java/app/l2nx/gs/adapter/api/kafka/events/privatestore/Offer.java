package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One open position in a player's private store at snapshot tick.
 * {@code (itemTemplateId, side)} is on the parent {@link PrivateStoreSnapshotEvent};
 * per-offer fields describe trader / modifiers / quantity / price.
 */
public final class Offer {

    private final @Nullable Long itemId;
    private final long traderId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<Attribute, Integer> attributes;
    private final long count;
    private final long unitPrice;
    private final long currencyItemTemplateId;
    private final @Nullable Boolean packaged;

    public Offer(
            @Nullable Long itemId,
            long traderId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPrice,
            long currencyItemTemplateId,
            @Nullable Boolean packaged) {
        this.itemId = itemId;
        this.traderId = traderId;
        this.enchantLevel = enchantLevel;
        this.attributes = freezeMap(attributes);
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemTemplateId = currencyItemTemplateId;
        this.packaged = packaged;
    }

    /**
     * Object id of this specific offered item instance (spec 065 §2.1).
     * {@code null} (or {@code 0}) for {@link PrivateStoreSide#BID BID}
     * offers, where it is meaningless — a BID offer targets a template, not a
     * specific instance. {@code null} on old producers that predate this
     * field.
     */
    public @Nullable Long getItemId() {
        return itemId;
    }

    /**
     * Store-owning player char id (seller on ASK, buyer on BID).
     */
    public long getTraderId() {
        return traderId;
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
     * Currency item template id (typically {@code 57} = Adena).
     */
    public long getCurrencyItemTemplateId() {
        return currencyItemTemplateId;
    }

    /**
     * {@code true} when this offer belongs to a PACKAGE_SELL store — the per-item price is
     * nominal, the real price is the bundle total. {@code null} when the host did not report
     * this (legacy); treat as {@code false}.
     */
    public @Nullable Boolean getPackaged() {
        return packaged;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .traderId(traderId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .count(count)
                .unitPrice(unitPrice)
                .currencyItemTemplateId(currencyItemTemplateId)
                .packaged(packaged);
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
        if (!(o instanceof Offer)) return false;
        Offer that = (Offer) o;
        return traderId == that.traderId
                && count == that.count
                && unitPrice == that.unitPrice
                && currencyItemTemplateId == that.currencyItemTemplateId
                && Objects.equals(itemId, that.itemId)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(packaged, that.packaged);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                itemId, traderId, enchantLevel, attributes, count, unitPrice, currencyItemTemplateId, packaged);
    }

    @Override
    public String toString() {
        return "Offer[itemId=" + itemId
                + ", traderId=" + traderId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemTemplateId=" + currencyItemTemplateId
                + ", packaged=" + packaged + "]";
    }

    public static final class Builder {
        private @Nullable Long itemId;
        private long traderId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;
        private long count;
        private long unitPrice;
        private long currencyItemTemplateId;
        private @Nullable Boolean packaged;

        public Builder itemId(@Nullable Long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder traderId(long traderId) {
            this.traderId = traderId;
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

        public Builder currencyItemTemplateId(long currencyItemTemplateId) {
            this.currencyItemTemplateId = currencyItemTemplateId;
            return this;
        }

        public Builder packaged(@Nullable Boolean packaged) {
            this.packaged = packaged;
            return this;
        }

        public Offer build() {
            return new Offer(
                    itemId, traderId, enchantLevel, attributes, count, unitPrice, currencyItemTemplateId, packaged);
        }
    }
}
