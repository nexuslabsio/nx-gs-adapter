package app.l2nx.gs.adapter.api.kafka.events.mail;

import app.l2nx.gs.adapter.api.domain.Attribute;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * One item-movement line carried by a mail-lifecycle event. Mirrors
 * {@link app.l2nx.gs.adapter.api.kafka.events.privatetrade.TradeItemMovement}.
 *
 * <p>{@link #getItemId() itemId} / {@link #getNewItemId() newItemId} describe
 * the object-id transition. SENT and RETURNED have no within-event transition
 * — both fields equal the mail attachment row id. ACCEPTED transitions
 * mail-row → receiver inventory; CANCELLED mail-row → sender inventory.
 * Stack-merge collapses {@code newItemId} onto an existing stack id.</p>
 */
public final class MailItemMovement {

    private final long itemTemplateId;
    private final long itemId;
    private final long newItemId;
    private final long count;
    private final @Nullable Integer enchantLevel;
    private final @Nullable Map<Attribute, Integer> attributes;

    public MailItemMovement(long itemTemplateId,
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
     * Object-id before the lifecycle step's transition.
     */
    public long getItemId() {
        return itemId;
    }

    /**
     * Object-id after the lifecycle step's transition.
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

    /**
     * Elemental attribute power per {@link Attribute}. Empty map on read
     * when none.
     */
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
        if (!(o instanceof MailItemMovement)) return false;
        MailItemMovement that = (MailItemMovement) o;
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
        return "MailItemMovement[itemTemplateId=" + itemTemplateId
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

        public MailItemMovement build() {
            return new MailItemMovement(itemTemplateId, itemId, newItemId, count, enchantLevel, attributes);
        }
    }
}
