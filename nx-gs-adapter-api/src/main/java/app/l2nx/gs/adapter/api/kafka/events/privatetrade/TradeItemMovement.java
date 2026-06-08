package app.l2nx.gs.adapter.api.kafka.events.privatetrade;

import app.l2nx.gs.adapter.api.domain.Attribute;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Transferred item line in a {@link PrivateTradeFinishedEvent}. Adena is
 * carried as a regular line (no separate price/currency field).
 */
public final class TradeItemMovement {

    private final long itemTemplateId;
    private final long itemId;
    private final long newItemId;
    private final long count;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<Attribute, Integer> attributes;

    public TradeItemMovement(long itemTemplateId,
                             long itemId,
                             long newItemId,
                             long count,
                             @Nullable Integer enchantLevel,
                             @Nullable Map<Attribute, Integer> attributes) {
        this.itemTemplateId = itemTemplateId;
        this.itemId = itemId;
        this.newItemId = newItemId;
        this.count = count;
        this.enchantLevel = enchantLevel;
        this.attributes = freezeMap(attributes);
    }

    public long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Giver-side inventory item object-id before the exchange.
     */
    public long getItemId() {
        return itemId;
    }

    /**
     * Receiver-side inventory item object-id after the exchange.
     */
    public long getNewItemId() {
        return newItemId;
    }

    public long getCount() {
        return count;
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

    public Builder toBuilder() {
        return new Builder()
                .itemTemplateId(itemTemplateId)
                .itemId(itemId)
                .newItemId(newItemId)
                .count(count)
                .enchantLevel(enchantLevel)
                .attributes(attributes);
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
        if (!(o instanceof TradeItemMovement)) return false;
        TradeItemMovement that = (TradeItemMovement) o;
        return itemTemplateId == that.itemTemplateId
                && itemId == that.itemId
                && newItemId == that.newItemId
                && count == that.count
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, itemId, newItemId, count, enchantLevel, attributes);
    }

    @Override
    public String toString() {
        return "TradeItemMovement[itemTemplateId=" + itemTemplateId
                + ", itemId=" + itemId
                + ", newItemId=" + newItemId
                + ", count=" + count
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes + "]";
    }

    public static final class Builder {
        private long itemTemplateId;
        private long itemId;
        private long newItemId;
        private long count;
        private @Nullable Integer enchantLevel;
        private @Nullable Map<Attribute, Integer> attributes;

        public Builder itemTemplateId(long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder itemId(long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder newItemId(long newItemId) {
            this.newItemId = newItemId;
            return this;
        }

        public Builder count(long count) {
            this.count = count;
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

        public TradeItemMovement build() {
            return new TradeItemMovement(itemTemplateId, itemId, newItemId, count, enchantLevel, attributes);
        }
    }
}
