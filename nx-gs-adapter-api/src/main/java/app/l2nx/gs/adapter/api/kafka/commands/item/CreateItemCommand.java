package app.l2nx.gs.adapter.api.kafka.commands.item;

import app.l2nx.gs.adapter.api.domain.item.ItemLocation;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Inbound command instructing the game-server to grant a fresh item stack to
 * a character. {@link #getItemTemplateId() itemTemplateId} is the catalog
 * template (e.g. {@code 57} = adena), NOT a stack object-id.
 *
 * <p>{@link #getEnchantLevel() enchantLevel} (optional, default {@code 0}) is
 * meaningful for non-stackable equipment; stackable templates ignore it.
 * {@link #getLocation() location} (optional, default {@link ItemLocation#INVENTORY})
 * picks the destination container. The handler MAY reject locations that
 * cannot be created into (e.g. {@code EQUIP}, {@code PET_EQUIP}, {@code MAIL})
 * with {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#INVALID_STATE}.</p>
 */
public final class CreateItemCommand implements NxCommand<CreateItemResult> {

    private final Long charId;
    private final Long itemTemplateId;
    private final Long count;
    private final @Nullable Long enchantLevel;
    private final @Nullable ItemLocation location;

    public CreateItemCommand(
            Long charId,
            Long itemTemplateId,
            Long count,
            @Nullable Long enchantLevel,
            @Nullable ItemLocation location) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (itemTemplateId == null) {
            throw new IllegalArgumentException("itemTemplateId is required");
        }
        if (count == null) {
            throw new IllegalArgumentException("count is required");
        }
        if (count <= 0L) {
            throw new IllegalArgumentException("count must be positive (got " + count + ")");
        }
        if (enchantLevel != null && enchantLevel < 0L) {
            throw new IllegalArgumentException("enchantLevel must be non-negative (got " + enchantLevel + ")");
        }
        this.charId = charId;
        this.itemTemplateId = itemTemplateId;
        this.count = count;
        this.enchantLevel = enchantLevel;
        this.location = location;
    }

    public Long getCharId() {
        return charId;
    }

    public Long getItemTemplateId() {
        return itemTemplateId;
    }

    public Long getCount() {
        return count;
    }

    public @Nullable Long getEnchantLevel() {
        return enchantLevel;
    }

    public @Nullable ItemLocation getLocation() {
        return location;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .itemTemplateId(itemTemplateId)
                .count(count)
                .enchantLevel(enchantLevel)
                .location(location);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CreateItemCommand)) return false;
        CreateItemCommand that = (CreateItemCommand) o;
        return Objects.equals(charId, that.charId)
                && Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(count, that.count)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && location == that.location;
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, itemTemplateId, count, enchantLevel, location);
    }

    @Override
    public String toString() {
        return "CreateItemCommand[charId=" + charId
                + ", itemTemplateId=" + itemTemplateId
                + ", count=" + count
                + ", enchantLevel=" + enchantLevel
                + ", location=" + location + "]";
    }

    public static final class Builder {
        private Long charId;
        private Long itemTemplateId;
        private Long count = 1L;
        private @Nullable Long enchantLevel;
        private @Nullable ItemLocation location;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder itemTemplateId(Long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public Builder enchantLevel(@Nullable Long enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder location(@Nullable ItemLocation location) {
            this.location = location;
            return this;
        }

        public CreateItemCommand build() {
            return new CreateItemCommand(charId, itemTemplateId, count, enchantLevel, location);
        }
    }
}
