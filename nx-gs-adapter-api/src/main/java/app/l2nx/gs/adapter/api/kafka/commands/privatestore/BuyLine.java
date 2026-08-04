package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One lot of a {@link BuyFromPrivateStoreCommand}, addressed <em>logically</em>
 * rather than by inventory instance object-id.
 *
 * <p><b>Why no object-id.</b> The order book published by the host
 * ({@code PrivateStoreSnapshotEvent} / {@code Offer}) carries no instance
 * object-id, so a remote buyer cannot name one. The host re-resolves the lot
 * inside the seller's live trade list by matching
 * {@code (itemTemplateId, enchantLevel, unitPriceAdena)} — plus
 * {@link #getAttributes() attributes} when non-empty — at the moment of the
 * deal. Indistinguishable twin lots resolve to the first match: the goods are
 * interchangeable by construction.</p>
 *
 * <p><b>Exact-match semantics.</b> The fields are an optimistic lock, not a
 * search filter: a price or enchant that no longer matches the live lot fails
 * the whole command with {@code OFFER_CHANGED} rather than buying something
 * else. Partial fills do not exist — see
 * {@link BuyFromPrivateStoreCommand}.</p>
 *
 * <p><b>Required fields.</b> {@link #getItemTemplateId() itemTemplateId},
 * {@link #getCount() count} and {@link #getUnitPriceAdena() unitPriceAdena} are
 * REQUIRED — the constructor enforces {@code count > 0} and
 * {@code unitPriceAdena >= 0} via {@link IllegalArgumentException} for
 * programmatic construction. Wire-path deserialization bypasses the constructor
 * — the handler re-checks and emits {@code VALIDATION_FAILED}.
 * {@link #getEnchantLevel() enchantLevel} and {@link #getAttributes()
 * attributes} are OPTIONAL — {@code null} means "the offer carried none", which
 * is itself part of the match.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class BuyLine {

    private final long itemTemplateId;
    private final @Nullable Integer enchantLevel;
    private final Map<Attribute, Integer> attributes;
    private final long count;
    private final long unitPriceAdena;

    public BuyLine(
            long itemTemplateId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPriceAdena) {
        if (count <= 0L) {
            throw new IllegalArgumentException("count must be positive (got " + count + ")");
        }
        if (unitPriceAdena < 0L) {
            throw new IllegalArgumentException("unitPriceAdena must be non-negative (got " + unitPriceAdena + ")");
        }
        this.itemTemplateId = itemTemplateId;
        this.enchantLevel = enchantLevel;
        this.attributes = PrivateStoreLists.freezeAttributes(attributes);
        this.count = count;
        this.unitPriceAdena = unitPriceAdena;
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
     * item templates that cannot be enchanted.
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
        return itemTemplateId == that.itemTemplateId
                && count == that.count
                && unitPriceAdena == that.unitPriceAdena
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, enchantLevel, attributes, count, unitPriceAdena);
    }

    @Override
    public String toString() {
        return "BuyLine[itemTemplateId=" + itemTemplateId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", count=" + count
                + ", unitPriceAdena=" + unitPriceAdena + "]";
    }

    public static final class Builder {
        private long itemTemplateId;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;
        private long count;
        private long unitPriceAdena;

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
            return new BuyLine(itemTemplateId, enchantLevel, attributes, count, unitPriceAdena);
        }
    }
}
