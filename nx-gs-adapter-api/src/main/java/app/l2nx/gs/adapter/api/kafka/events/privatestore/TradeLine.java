package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One position of a closed {@link PrivateStoreTradeEvent}. A single trade
 * (one buyer-click in a seller's store, or vice versa) atomically transfers
 * any number of distinct positions; each becomes a {@code TradeLine} on the
 * wire so consumers can pivot on per-item analytics without re-deriving
 * line-level data from a flattened total.
 *
 * <p><b>Item identity is multi-dimensional in L2.</b> Two
 * {@code (itemId, enchantLevel, elementalAttrs)} triples describe genuinely
 * different "instruments" with prices that can differ by orders of magnitude
 * (a {@code +0} Draconic Bow vs a {@code +16 fire-300} Draconic Bow). The
 * triple is published verbatim — the platform consumer chooses the pivot
 * granularity (per {@code itemId} for fungibles, per
 * {@code (itemId, enchantLevel)} for base gear, per full triple for top-tier
 * gear).</p>
 *
 * <p>Augmentation, soul-crystal stage, and special-ability modifiers are NOT
 * carried in v1 — only {@link #getEnchantLevel() enchantLevel} and
 * {@link #getElementalAttrs() elementalAttrs} are surfaced. Adding modifier
 * fields later is a non-breaking minor-version change.</p>
 */
public final class TradeLine {

    private final long itemId;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<String, Integer> elementalAttrs;
    private final long count;
    private final long unitPrice;
    private final long currencyItemId;

    public TradeLine(long itemId,
                     @Nullable Integer enchantLevel,
                     @Nullable Map<String, Integer> elementalAttrs,
                     long count,
                     long unitPrice,
                     long currencyItemId) {
        this.itemId = itemId;
        this.enchantLevel = enchantLevel;
        this.elementalAttrs = freezeMap(elementalAttrs);
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
     * Enchant level of the traded item. {@code null} when the item type has
     * no enchant concept (consumables, materials, recipes); {@code 0} for an
     * enchantable item that has not been enchanted; {@code > 0} otherwise.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /**
     * Elemental attribute power, keyed by attribute name. Always non-null on
     * read; {@code null} or empty passed to the constructor is normalized to
     * an empty map.
     *
     * <p>Keys: see {@link WellKnownElements} for the canonical L2 set
     * ({@code fire}, {@code water}, {@code earth}, {@code wind}, {@code holy},
     * {@code dark}). Values are positive integers (attribute points).</p>
     */
    public Map<String, Integer> getElementalAttrs() {
        return elementalAttrs == null ? Collections.emptyMap() : elementalAttrs;
    }

    /**
     * Quantity of the item in this position.
     *
     * <p>Soft invariant: {@code count > 0}. The constructor accepts
     * {@code 0} and negative values to keep the POJO Gson-friendly, but
     * producers MUST NOT emit non-positive lines. Consumer-side validation
     * logs and dedupes; the wire schema permits the value.</p>
     */
    public long getCount() {
        return count;
    }

    /**
     * Per-unit price denominated in {@link #getCurrencyItemId() currencyItemId}.
     *
     * <p>Soft invariant: {@code unitPrice >= 0}. The constructor accepts
     * negative values for Gson tolerance; producers MUST NOT emit
     * negatives.</p>
     */
    public long getUnitPrice() {
        return unitPrice;
    }

    /**
     * L2 item ID acting as the currency for this line. Typically
     * {@code 57} (Adena); the schema is general so alt-currency stores
     * (event coins, donate coins) can be modeled by hosts that support them.
     */
    public long getCurrencyItemId() {
        return currencyItemId;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
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
        if (!(o instanceof TradeLine)) return false;
        TradeLine that = (TradeLine) o;
        return itemId == that.itemId
                && count == that.count
                && unitPrice == that.unitPrice
                && currencyItemId == that.currencyItemId
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(elementalAttrs, that.elementalAttrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, enchantLevel, elementalAttrs, count, unitPrice, currencyItemId);
    }

    @Override
    public String toString() {
        return "TradeLine[itemId=" + itemId
                + ", enchantLevel=" + enchantLevel
                + ", elementalAttrs=" + elementalAttrs
                + ", count=" + count
                + ", unitPrice=" + unitPrice
                + ", currencyItemId=" + currencyItemId + "]";
    }

    public static final class Builder {
        private long itemId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<String, Integer> elementalAttrs;
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

        public TradeLine build() {
            return new TradeLine(itemId, enchantLevel, elementalAttrs, count, unitPrice, currencyItemId);
        }
    }
}
