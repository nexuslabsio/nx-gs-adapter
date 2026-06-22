package app.l2nx.gs.adapter.api.kafka.commands.item;

import app.l2nx.gs.adapter.api.domain.item.ItemLocation;
import java.util.Objects;

/**
 * Success payload of {@link CreateItemCommand}. {@link #getItemId() itemId}
 * is the host-assigned object-id of the resulting stack — when an existing
 * stackable stack absorbed the grant, this is that stack's id rather than a
 * fresh one. {@link #getCountCreated() countCreated} echoes the actual
 * delta applied (host MAY clamp on stack-size limits).
 */
public final class CreateItemResult {

    private final Long itemId;
    private final Long countCreated;
    private final Long enchantLevel;
    private final ItemLocation location;

    public CreateItemResult(Long itemId, Long countCreated, Long enchantLevel, ItemLocation location) {
        if (itemId == null) {
            throw new IllegalArgumentException("itemId is required");
        }
        if (countCreated == null) {
            throw new IllegalArgumentException("countCreated is required");
        }
        if (countCreated <= 0L) {
            throw new IllegalArgumentException("countCreated must be positive (got " + countCreated + ")");
        }
        if (enchantLevel == null) {
            throw new IllegalArgumentException("enchantLevel is required");
        }
        if (location == null) {
            throw new IllegalArgumentException("location is required");
        }
        this.itemId = itemId;
        this.countCreated = countCreated;
        this.enchantLevel = enchantLevel;
        this.location = location;
    }

    public Long getItemId() {
        return itemId;
    }

    public Long getCountCreated() {
        return countCreated;
    }

    public Long getEnchantLevel() {
        return enchantLevel;
    }

    public ItemLocation getLocation() {
        return location;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .countCreated(countCreated)
                .enchantLevel(enchantLevel)
                .location(location);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CreateItemResult)) return false;
        CreateItemResult that = (CreateItemResult) o;
        return Objects.equals(itemId, that.itemId)
                && Objects.equals(countCreated, that.countCreated)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && location == that.location;
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, countCreated, enchantLevel, location);
    }

    @Override
    public String toString() {
        return "CreateItemResult[itemId=" + itemId
                + ", countCreated=" + countCreated
                + ", enchantLevel=" + enchantLevel
                + ", location=" + location + "]";
    }

    public static final class Builder {
        private Long itemId;
        private Long countCreated;
        private Long enchantLevel;
        private ItemLocation location;

        public Builder itemId(Long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder countCreated(Long countCreated) {
            this.countCreated = countCreated;
            return this;
        }

        public Builder enchantLevel(Long enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder location(ItemLocation location) {
            this.location = location;
            return this;
        }

        public CreateItemResult build() {
            return new CreateItemResult(itemId, countCreated, enchantLevel, location);
        }
    }
}
