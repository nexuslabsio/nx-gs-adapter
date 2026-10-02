package app.l2nx.gs.adapter.api.kafka.commands.privatestore.model;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One lot of a {@link BuyFromPrivateStoreCommand}. The host resolves it by {@code itemId} (instance object-id), then
 * re-verifies template, enchant, attributes and price - the same instance may have been re-enchanted in place.
 * Fields beyond {@code itemId} are an optimistic lock: any mismatch fails the whole command with
 * {@code OFFER_CHANGED}; there are no partial fills. {@code null} enchant/attributes means "offer carried none" and is
 * part of the match.
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
        this.attributes = freezeAttributes(attributes);
        this.count = count;
        this.unitPriceAdena = unitPriceAdena;
    }

    /** Instance object-id, NOT the catalog template id. */
    public int getItemId() {
        return itemId;
    }

    /** Catalog template id, NOT an instance object-id. */
    public long getItemTemplateId() {
        return itemTemplateId;
    }

    /** {@code null} for non-enchantable templates; otherwise {@code 0..127}. */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /** Empty when the offer carried none; matched only when non-empty. */
    public Map<Attribute, Integer> getAttributes() {
        return attributes;
    }

    /** The host buys exactly this many or fails; it never shrinks the count. */
    public long getCount() {
        return count;
    }

    /** Must match the live lot exactly. */
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

    private static Map<Attribute, Integer> freezeAttributes(@Nullable Map<Attribute, Integer> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new EnumMap<Attribute, Integer>(src));
    }
}
