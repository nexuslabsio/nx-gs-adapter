package app.l2nx.gs.adapter.api.kafka.sync.db.item;

import app.l2nx.gs.adapter.api.domain.item.ItemLocation;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one item, payload of {@code SyncEvent<ItemDbDto>}. Only {@code id} is required;
 * {@code ownerId} is null for the {@code owner_id = 0} sentinel.
 *
 * {@code attributes} is null when the tenant does not sync elementals, empty when synced but none
 * (Gson omits nulls, so the wire distinguishes the two).
 */
public final class ItemDbDto {

    private final long id;
    private final @Nullable Long itemTemplateId;
    private final @Nullable Long ownerId;
    private final @Nullable Long count;
    private final @Nullable Integer enchantLevel;
    private final @Nullable ItemLocation location;
    private final @Nullable List<ItemAttributeDbDto> attributes;
    private final @Nullable ItemAugmentationDbDto augmentation;

    public ItemDbDto(
            long id,
            @Nullable Long itemTemplateId,
            @Nullable Long ownerId,
            @Nullable Long count,
            @Nullable Integer enchantLevel,
            @Nullable ItemLocation location,
            @Nullable List<ItemAttributeDbDto> attributes,
            @Nullable ItemAugmentationDbDto augmentation) {
        this.id = id;
        this.itemTemplateId = itemTemplateId;
        this.ownerId = ownerId;
        this.count = count;
        this.enchantLevel = enchantLevel;
        this.location = location;
        this.attributes = attributes == null ? null : Collections.unmodifiableList(attributes);
        this.augmentation = augmentation;
    }

    public long getId() {
        return id;
    }

    /** Id in the static item catalog. */
    public @Nullable Long getItemTemplateId() {
        return itemTemplateId;
    }

    /** Player or clan, depending on {@code location}. */
    public @Nullable Long getOwnerId() {
        return ownerId;
    }

    public @Nullable Long getCount() {
        return count;
    }

    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public @Nullable ItemLocation getLocation() {
        return location;
    }

    public @Nullable List<ItemAttributeDbDto> getAttributes() {
        return attributes;
    }

    /** Null when not augmented or when the tenant does not sync augmentation. */
    public @Nullable ItemAugmentationDbDto getAugmentation() {
        return augmentation;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .itemTemplateId(itemTemplateId)
                .ownerId(ownerId)
                .count(count)
                .enchantLevel(enchantLevel)
                .location(location)
                .attributes(attributes)
                .augmentation(augmentation);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemDbDto)) return false;
        ItemDbDto that = (ItemDbDto) o;
        return id == that.id
                && Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(ownerId, that.ownerId)
                && Objects.equals(count, that.count)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && location == that.location
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(augmentation, that.augmentation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, itemTemplateId, ownerId, count, enchantLevel, location, attributes, augmentation);
    }

    @Override
    public String toString() {
        return "ItemDbDto[id=" + id
                + ", itemTemplateId=" + itemTemplateId
                + ", ownerId=" + ownerId
                + ", count=" + count
                + ", enchantLevel=" + enchantLevel
                + ", location=" + location
                + ", attributes=" + attributes
                + ", augmentation=" + augmentation + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable Long itemTemplateId;
        private @Nullable Long ownerId;
        private @Nullable Long count;
        private @Nullable Integer enchantLevel;
        private @Nullable ItemLocation location;
        private @Nullable List<ItemAttributeDbDto> attributes;
        private @Nullable ItemAugmentationDbDto augmentation;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder itemTemplateId(@Nullable Long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder ownerId(@Nullable Long ownerId) {
            this.ownerId = ownerId;
            return this;
        }

        public Builder count(@Nullable Long count) {
            this.count = count;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder location(@Nullable ItemLocation location) {
            this.location = location;
            return this;
        }

        public Builder attributes(@Nullable List<ItemAttributeDbDto> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder augmentation(@Nullable ItemAugmentationDbDto augmentation) {
            this.augmentation = augmentation;
            return this;
        }

        public ItemDbDto build() {
            return new ItemDbDto(id, itemTemplateId, ownerId, count, enchantLevel, location, attributes, augmentation);
        }
    }
}
