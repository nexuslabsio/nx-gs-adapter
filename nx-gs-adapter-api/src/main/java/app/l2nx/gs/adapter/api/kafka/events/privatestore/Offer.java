package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One offer entry inside a {@link PrivateStoreSnapshotEvent}'s
 * {@link PrivateStoreSnapshotEvent#getOffers() offers} list. Represents a
 * single open position in some player's private store at the moment the
 * snapshot tick was taken.
 *
 * <p>The {@code (itemId, side)} addressing belongs to the parent
 * {@code PrivateStoreSnapshotEvent} — every offer in one snapshot event
 * shares the same item and side. Per-offer fields here describe the
 * <i>per-store</i> dimensions: who is offering, with what modifiers, in what
 * quantity, at what price.</p>
 *
 * <p>{@link #getEnchantLevel() enchantLevel} +
 * {@link #getElementalAttrs() elementalAttrs} are part of the offer key —
 * two stores listing the same {@code itemId} at different enchant levels are
 * two distinct offers, NOT one collapsed entry. The platform consumer chooses
 * how to aggregate.</p>
 */
public final class Offer {

    private final long traderId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<String, Integer> elementalAttrs;
    private final long count;
    private final long unitPrice;
    private final long currencyItemId;

    public Offer(long traderId,
                 @Nullable Integer enchantLevel,
                 @Nullable Map<String, Integer> elementalAttrs,
                 long count,
                 long unitPrice,
                 long currencyItemId) {
        this.traderId = traderId;
        this.enchantLevel = enchantLevel;
        this.elementalAttrs = freezeMap(elementalAttrs);
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
    }

    /**
     * Source-side character ID of the player whose private store is hosting
     * this offer. The trader is the seller on
     * {@link PrivateStoreSide#ASK ASK}-side snapshots, the buyer on
     * {@link PrivateStoreSide#BID BID}-side snapshots.
     */
    public long getTraderId() {
        return traderId;
    }

    /**
     * Enchant level of the offered item. {@code null} when the item type has
     * no enchant concept (consumables, materials, recipes); {@code 0} for an
     * enchantable item that has not been enchanted; {@code > 0} otherwise.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /**
     * Elemental attribute power, keyed by attribute name. Always non-null on
     * read; {@code null} or empty passed to the constructor is normalized to
     * an empty map. See {@link WellKnownElements} for canonical keys.
     */
    public Map<String, Integer> getElementalAttrs() {
        return elementalAttrs == null ? Collections.emptyMap() : elementalAttrs;
    }

    /**
     * Remaining quantity available at this offer at the snapshot tick.
     *
     * <p>Soft invariant: {@code count > 0}. An offer that drains to zero
     * disappears from the next snapshot rather than being published with
     * {@code count=0}.</p>
     */
    public long getCount() {
        return count;
    }

    /**
     * Per-unit price denominated in {@link #getCurrencyItemId() currencyItemId}.
     */
    public long getUnitPrice() {
        return unitPrice;
    }

    /**
     * L2 item ID acting as the currency for this offer. Typically
     * {@code 57} (Adena).
     */
    public long getCurrencyItemId() {
        return currencyItemId;
    }

    public Builder toBuilder() {
        return new Builder()
                .traderId(traderId)
                .enchantLevel(enchantLevel)
                .elementalAttrs(elementalAttrs)
                .count(count)
                .unitPrice(unitPrice)
                .currencyItemId(currencyItemId);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static @Nullable Map<String, Integer> freezeMap(@Nullable Map<String, Integer> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(src));
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
                && Objects.equals(elementalAttrs, that.elementalAttrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traderId, enchantLevel, elementalAttrs, count, unitPrice, currencyItemId);
    }

    @Override
    public String toString() {
        return "Offer[traderId=" + traderId
                + ", enchantLevel=" + enchantLevel
                + ", elementalAttrs=" + elementalAttrs
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemId=" + currencyItemId + "]";
    }

    public static final class Builder {
        private long traderId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<String, Integer> elementalAttrs;
        private long count;
        private long unitPrice;
        private long currencyItemId;

        public Builder traderId(long traderId) {
            this.traderId = traderId;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder elementalAttrs(@Nullable Map<String, Integer> elementalAttrs) {
            this.elementalAttrs = elementalAttrs;
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

        public Offer build() {
            return new Offer(traderId, enchantLevel, elementalAttrs, count, unitPrice, currencyItemId);
        }
    }
}
