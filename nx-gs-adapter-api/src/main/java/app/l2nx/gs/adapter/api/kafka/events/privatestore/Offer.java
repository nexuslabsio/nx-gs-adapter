package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One open position in a player's private store at snapshot tick.
 * {@code (itemId, side)} is on the parent {@link PrivateStoreSnapshotEvent};
 * per-offer fields describe trader / modifiers / quantity / price.
 */
public final class Offer {

    private final long traderId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<Attribute, Integer> attributes;
    private final long count;
    private final long unitPrice;
    private final long currencyItemId;
    private final @Nullable Boolean packaged;

    public Offer(
            long traderId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPrice,
            long currencyItemId,
            @Nullable Boolean packaged) {
        this.traderId = traderId;
        this.enchantLevel = enchantLevel;
        this.attributes = freezeMap(attributes);
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
        this.packaged = packaged;
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
     * Currency item id (typically {@code 57} = Adena).
     */
    public long getCurrencyItemId() {
        return currencyItemId;
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
                .traderId(traderId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .count(count)
                .unitPrice(unitPrice)
                .currencyItemId(currencyItemId)
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
                && currencyItemId == that.currencyItemId
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(packaged, that.packaged);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traderId, enchantLevel, attributes, count, unitPrice, currencyItemId, packaged);
    }

    @Override
    public String toString() {
        return "Offer[traderId=" + traderId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemId=" + currencyItemId
                + ", packaged=" + packaged + "]";
    }

    public static final class Builder {
        private long traderId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;
        private long count;
        private long unitPrice;
        private long currencyItemId;
        private @Nullable Boolean packaged;

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

        public Builder currencyItemId(long currencyItemId) {
            this.currencyItemId = currencyItemId;
            return this;
        }

        public Builder packaged(@Nullable Boolean packaged) {
            this.packaged = packaged;
            return this;
        }

        public Offer build() {
            return new Offer(traderId, enchantLevel, attributes, count, unitPrice, currencyItemId, packaged);
        }
    }
}
