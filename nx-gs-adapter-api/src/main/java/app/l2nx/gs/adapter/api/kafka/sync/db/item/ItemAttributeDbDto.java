package app.l2nx.gs.adapter.api.kafka.sync.db.item;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Objects;

/** Wire DTO for one {@code item_elementals} row, carried in {@link ItemDbDto#getAttributes()}. */
public final class ItemAttributeDbDto {

    private final Attribute type;
    private final int value;

    public ItemAttributeDbDto(Attribute type, int value) {
        this.type = type;
        this.value = value;
    }

    public Attribute getType() {
        return type;
    }

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
        if (!(o instanceof ItemAttributeDbDto)) return false;
        ItemAttributeDbDto that = (ItemAttributeDbDto) o;
        return type == that.type && value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return "ItemAttributeDbDto[type=" + type + ", value=" + value + "]";
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

        public ItemAttributeDbDto build() {
            return new ItemAttributeDbDto(type, value);
        }
    }
}
