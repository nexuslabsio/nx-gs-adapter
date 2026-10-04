package app.l2nx.gs.adapter.api.kafka.events.chat;

import app.l2nx.gs.adapter.api.kafka.sync.db.item.ItemAugmentationDbDto;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public final class ChatItemSnapshot {

    private final long itemObjectId;
    private final int itemTemplateId;
    private final int enchantLevel;
    private final Map<String, Integer> attributes;
    private final @Nullable ItemAugmentationDbDto augmentation;

    public ChatItemSnapshot(
            long itemObjectId,
            int itemTemplateId,
            int enchantLevel,
            @Nullable Map<String, Integer> attributes,
            @Nullable ItemAugmentationDbDto augmentation) {
        this.itemObjectId = itemObjectId;
        this.itemTemplateId = itemTemplateId;
        this.enchantLevel = enchantLevel;
        this.attributes = attributes == null
                ? Collections.<String, Integer>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(attributes));
        this.augmentation = augmentation;
    }

    /** The {@code ID=} value of the item token in the message text. */
    public long getItemObjectId() {
        return itemObjectId;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public int getEnchantLevel() {
        return enchantLevel;
    }

    public Map<String, Integer> getAttributes() {
        return attributes;
    }

    public @Nullable ItemAugmentationDbDto getAugmentation() {
        return augmentation;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemObjectId(itemObjectId)
                .itemTemplateId(itemTemplateId)
                .enchantLevel(enchantLevel)
                .attributes(attributes)
                .augmentation(augmentation);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChatItemSnapshot)) return false;
        ChatItemSnapshot that = (ChatItemSnapshot) o;
        return itemObjectId == that.itemObjectId
                && itemTemplateId == that.itemTemplateId
                && enchantLevel == that.enchantLevel
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(augmentation, that.augmentation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemObjectId, itemTemplateId, enchantLevel, attributes, augmentation);
    }

    @Override
    public String toString() {
        return "ChatItemSnapshot[itemObjectId=" + itemObjectId
                + ", itemTemplateId=" + itemTemplateId
                + ", enchantLevel=" + enchantLevel
                + ", attributes=" + attributes
                + ", augmentation=" + augmentation + "]";
    }

    public static final class Builder {
        private long itemObjectId;
        private int itemTemplateId;
        private int enchantLevel;
        private @Nullable Map<String, Integer> attributes;
        private @Nullable ItemAugmentationDbDto augmentation;

        public Builder itemObjectId(long itemObjectId) {
            this.itemObjectId = itemObjectId;
            return this;
        }

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder enchantLevel(int enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder attributes(@Nullable Map<String, Integer> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder augmentation(@Nullable ItemAugmentationDbDto augmentation) {
            this.augmentation = augmentation;
            return this;
        }

        public ChatItemSnapshot build() {
            return new ChatItemSnapshot(itemObjectId, itemTemplateId, enchantLevel, attributes, augmentation);
        }
    }
}
