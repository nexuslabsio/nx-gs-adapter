package app.l2nx.gs.adapter.api.kafka.sync.db;

import app.l2nx.gs.adapter.api.domain.Attribute;

import java.util.Objects;

/**
 * Wire DTO for one row of {@code item_elementals} (or its tenant-equivalent),
 * carried inside {@link ItemDto#getAttributes()}.
 *
 * <p>Surfaces the elemental kind ({@link Attribute}) and the source-side
 * numeric strength ({@code value}). The composite source-side key is
 * {@code (itemId, elemType)} — the {@code itemId} part is implicit (the
 * parent {@link ItemDto}'s {@code id}); only {@code type} disambiguates
 * rows within one item's attributes list.</p>
 */
public final class ItemAttributeDto {

    private final Attribute type;
    private final int value;

    public ItemAttributeDto(Attribute type, int value) {
        this.type = type;
        this.value = value;
    }

    /**
     * Element kind — {@code NOT NULL} on the source side.
     */
    public Attribute getType() {
        return type;
    }

    /**
     * Element strength — {@code NOT NULL} on the source side; source default
     * {@code -1}.
     */
    public int getValue() {
        return value;
    }

    public Builder toBuilder() {
        return new Builder().type(type).value(value);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemAttributeDto)) return false;
        ItemAttributeDto that = (ItemAttributeDto) o;
        return type == that.type && value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return "ItemAttributeDto[type=" + type + ", value=" + value + "]";
    }

    public static final class Builder {
        private Attribute type;
        private int value;

        public Builder type(Attribute type) {
            this.type = type;
            return this;
        }

        public Builder value(int value) {
            this.value = value;
            return this;
        }

        public ItemAttributeDto build() {
            return new ItemAttributeDto(type, value);
        }
    }
}
