package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One lot of a {@link BuyFromPrivateStoreCommand}, addressed by the exact
 * inventory instance the buyer saw in the market book.
 *
 * <p><b>{@code itemId} is the primary identity key.</b> The host resolves this
 * lot in the seller's live trade list by {@code itemId} (the instance
 * object-id) first, then re-verifies {@code (itemTemplateId, enchantLevel,
 * attributes, unitPriceAdena)} against the resolved item — the same instance
 * may have been re-enchanted or re-attributed in place since the buyer last
 * saw it, so an object-id match alone is not sufficient.</p>
 *
 * <p><b>Exact-match semantics.</b> The fields beyond {@code itemId} are an
 * optimistic lock, not a search filter: a price, enchant, or attribute that no
 * longer matches the live lot fails the whole command with
 * {@code OFFER_CHANGED} rather than buying something else. Partial fills do
 * not exist — see {@link BuyFromPrivateStoreCommand}.</p>
 *
 * <p><b>Required fields.</b> {@link #getItemId() itemId},
 * {@link #getItemTemplateId() itemTemplateId}, {@link #getCount() count} and
 * {@link #getUnitPriceAdena() unitPriceAdena} are REQUIRED — the constructor
 * enforces {@code itemId > 0}, {@code itemTemplateId > 0}, {@code count > 0},
 * {@code unitPriceAdena >= 0}, and that {@code count * unitPriceAdena} does not
 * overflow a {@code long}, via {@link IllegalArgumentException} for
 * programmatic construction. Wire-path deserialization bypasses the constructor
 * — the handler re-checks and emits {@code VALIDATION_FAILED}.
 * {@link #getEnchantLevel() enchantLevel} and {@link #getAttributes()
 * attributes} are OPTIONAL — {@code null} means "the offer carried none", which
 * is itself part of the match. When present, {@code enchantLevel} MUST be in
 * {@code 0..127}.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class BuyLine {

    private final int itemId;
    private final long itemTemplateId;
    private final @Nullable Integer enchantLevel;
    private final Map<Attribute, Integer> attributes;
    private final long count;
    private final long unitPriceAdena;

    public BuyLine(
            int itemId,
            long itemTemplateId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPriceAdena) {
        if (itemId <= 0) {
            throw new IllegalArgumentException("itemId must be positive (got " + itemId + ")");
        }
        if (itemTemplateId <= 0L) {
            throw new IllegalArgumentException("itemTemplateId must be positive (got " + itemTemplateId + ")");
        }
        if (count <= 0L) {
            throw new IllegalArgumentException("count must be positive (got " + count + ")");
        }
        if (unitPriceAdena < 0L) {
            throw new IllegalArgumentException("unitPriceAdena must be non-negative (got " + unitPriceAdena + ")");
        }
        try {
            Math.multiplyExact(count, unitPriceAdena);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "count * unitPriceAdena overflows a long (count=" + count + ", unitPriceAdena=" + unitPriceAdena
                            + ")",
                    e);
        }
        if (enchantLevel != null && (enchantLevel < 0 || enchantLevel > 127)) {
            throw new IllegalArgumentException("enchantLevel must be in 0..127 (got " + enchantLevel + ")");
        }
        this.itemId = itemId;
        this.itemTemplateId = itemTemplateId;
        this.enchantLevel = enchantLevel;
        this.attributes = PrivateStoreLists.freezeAttributes(attributes);
        this.count = count;
        this.unitPriceAdena = unitPriceAdena;
    }

    /**
     * Object id of the specific item instance the buyer saw in the market
     * book — the primary lot identity key (NOT the catalog item-template id).
     */
    public int getItemId() {
        return itemId;
    }

    /**
     * Catalog item-template id of the offered item (NOT an inventory instance
     * object-id).
     */
    public long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Enchant level the offer was published with. OPTIONAL — {@code null} for
     * item templates that cannot be enchanted. When present, in {@code 0..127}.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /**
     * Elemental attributes the offer was published with. Empty when the offer
     * carried none; participates in lot matching only when non-empty.
     * Immutable on read.
     */
    public Map<Attribute, Integer> getAttributes() {
        return attributes;
    }

    /**
     * Units to buy. REQUIRED, MUST be positive. The host buys exactly this
     * many or fails the command — it never silently shrinks the count to what
     * is still available.
     */
    public long getCount() {
        return count;
    }

    /**
     * Per-unit adena price as published in the offer. REQUIRED, MUST be
     * non-negative. Must match the live lot exactly.
     */
    public long getUnitPriceAdena() {
        return unitPriceAdena;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .itemTemplateId(itemTemplateId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .count(count)
                .unitPriceAdena(unitPriceAdena);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BuyLine)) return false;
        BuyLine that = (BuyLine) o;
        return itemId == that.itemId
                && itemTemplateId == that.itemTemplateId
                && count == that.count
                && unitPriceAdena == that.unitPriceAdena
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, itemTemplateId, enchantLevel, attributes, count, unitPriceAdena);
    }

    @Override
    public String toString() {
        return "BuyLine[itemId=" + itemId
                + ", itemTemplateId=" + itemTemplateId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPriceAdena=" + unitPriceAdena + "]";
    }

    public static final class Builder {
        private int itemId;
        private long itemTemplateId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;
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

        public Builder attributes(@Nullable Map<Attribute, Integer> attributes) {
            this.attributes = attributes;
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

        public BuyLine build() {
            return new BuyLine(itemId, itemTemplateId, enchantLevel, attributes, count, unitPriceAdena);
        }
    }
}
