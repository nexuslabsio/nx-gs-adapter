package app.l2nx.gs.adapter.api.kafka.sync.db.item;

import app.l2nx.gs.adapter.api.domain.item.ItemLocation;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wire DTO for one item, payload of {@code SyncEvent<ItemDbDto>} on the
 * platform-supplied per-tenant item sync topic
 * (e.g. {@code bohpts.gs.sync.items}).
 *
 * <p>Only the primary key {@code id} (source-side {@code object_id}) is
 * required; everything else is optional. Different tenants populate
 * different subsets depending on which columns exist in their schema and
 * which the tenant chose to surface — schema providers control this via
 * {@code PrimarySource.hashedColumns()} and what they put into the row in
 * {@code mapRow()}.</p>
 *
 * <p>Sentinel mapping: most game-server schemas use {@code 0} as the
 * "no owner" sentinel in {@code items.owner_id}. Schema providers translate
 * sentinel-zero to {@code null} when populating {@code ownerId}; platform
 * consumers see explicit nulls.</p>
 *
 * <p>The {@code attributes} list aggregates child rows from the tenant's
 * {@code item_elementals}-equivalent table assembled by the schema
 * provider's {@code mapEntity}. {@code null} when the tenant does not sync
 * elementals at all (no {@code ChildSource} declared); empty list when the
 * tenant syncs elementals but the item has none. Gson's default
 * {@code serializeNulls=false} omits the field from JSON when {@code null},
 * so the wire shape unambiguously distinguishes "feature not synced" from
 * "feature synced, value empty".</p>
 */
public final class ItemDbDto {

    private final long id;
    private final @Nullable Long itemTemplateId;
    private final @Nullable Long ownerId;
    private final @Nullable Long count;
    private final @Nullable Integer enchantLevel;
    private final @Nullable ItemLocation location;
    private final @Nullable List<ItemAttributeDbDto> attributes;

    public ItemDbDto(long id,
                     @Nullable Long itemTemplateId,
                     @Nullable Long ownerId,
                     @Nullable Long count,
                     @Nullable Integer enchantLevel,
                     @Nullable ItemLocation location,
                     @Nullable List<ItemAttributeDbDto> attributes) {
        this.id = id;
        this.itemTemplateId = itemTemplateId;
        this.ownerId = ownerId;
        this.count = count;
        this.enchantLevel = enchantLevel;
        this.location = location;
        this.attributes = attributes == null ? null : Collections.unmodifiableList(attributes);
    }

    /**
     * Primary key — source {@code object_id}, {@code NOT NULL}.
     */
    public long getId() {
        return id;
    }

    /**
     * Item template id — source {@code item_id}, points to the static item
     * catalog. {@code null} when the tenant does not surface this column or
     * when the source value is SQL NULL.
     */
    public @Nullable Long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Owner identifier (player or clan, depending on the {@code location}).
     * {@code null} when the source {@code owner_id = 0} (the conventional
     * "no owner" sentinel) or when the tenant does not surface this column.
     */
    public @Nullable Long getOwnerId() {
        return ownerId;
    }

    /**
     * Stack size (always non-negative on the source side).
     */
    public @Nullable Long getCount() {
        return count;
    }

    /**
     * Enchant level.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /**
     * Storage location.
     */
    public @Nullable ItemLocation getLocation() {
        return location;
    }

    /**
     * Item elemental attributes, ordered as the schema provider's
     * {@code mapEntity} produced them (no platform-side ordering contract).
     * {@code null} when the tenant does not sync attributes (no
     * {@code ChildSource} declared); empty list when the tenant syncs
     * attributes but the item has none.
     */
    public @Nullable List<ItemAttributeDbDto> getAttributes() {
        return attributes;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .itemTemplateId(itemTemplateId)
                .ownerId(ownerId)
                .count(count)
                .enchantLevel(enchantLevel)
                .location(location)
                .attributes(attributes);
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
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, itemTemplateId, ownerId, count, enchantLevel, location, attributes);
    }

    @Override
    public String toString() {
        return "ItemDbDto[id=" + id
                + ", itemTemplateId=" + itemTemplateId
                + ", ownerId=" + ownerId
                + ", count=" + count
                + ", enchantLevel=" + enchantLevel
                + ", location=" + location
                + ", attributes=" + attributes + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable Long itemTemplateId;
        private @Nullable Long ownerId;
        private @Nullable Long count;
        private @Nullable Integer enchantLevel;
        private @Nullable ItemLocation location;
        private @Nullable List<ItemAttributeDbDto> attributes;

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

        public ItemDbDto build() {
            return new ItemDbDto(id, itemTemplateId, ownerId, count, enchantLevel, location, attributes);
        }
    }
}
