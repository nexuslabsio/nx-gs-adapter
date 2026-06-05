package app.l2nx.gs.adapter.api.kafka.sync.gd.specialabilitytemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One gemstone/adena cost line for operating on a {@link SpecialAbilityTemplate} stone's
 * grade. {@code kind} is the canonical UPPER_SNAKE operation ({@code ADD}/{@code CHANGE}/
 * {@code REMOVE}); {@code slotIndex} is the ability slot the cost applies to for
 * {@code ADD}/{@code CHANGE} ({@code null} for {@code REMOVE}); {@code itemTemplateId} +
 * {@code count} are the consumed item and quantity.
 */
public final class SpecialAbilityPrice {

    private final String kind;
    private final @Nullable Integer slotIndex;
    private final int itemTemplateId;
    private final @Nullable Long count;

    private SpecialAbilityPrice(Builder b) {
        this.kind = Objects.requireNonNull(b.kind, "kind");
        this.slotIndex = b.slotIndex;
        this.itemTemplateId = b.itemTemplateId;
        this.count = b.count;
    }

    /**
     * Operation this cost applies to: {@code ADD} / {@code CHANGE} / {@code REMOVE}.
     */
    public String getKind() {
        return kind;
    }

    /**
     * Ability slot index the cost applies to for {@code ADD}/{@code CHANGE}; {@code null}
     * for {@code REMOVE}.
     */
    public @Nullable Integer getSlotIndex() {
        return slotIndex;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Long getCount() {
        return count;
    }

    public Builder toBuilder() {
        return new Builder()
                .kind(kind)
                .slotIndex(slotIndex)
                .itemTemplateId(itemTemplateId)
                .count(count);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpecialAbilityPrice)) return false;
        SpecialAbilityPrice that = (SpecialAbilityPrice) o;
        return itemTemplateId == that.itemTemplateId
                && Objects.equals(kind, that.kind)
                && Objects.equals(slotIndex, that.slotIndex)
                && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, slotIndex, itemTemplateId, count);
    }

    @Override
    public String toString() {
        return "SpecialAbilityPrice[kind=" + kind + ", slotIndex=" + slotIndex
                + ", itemTemplateId=" + itemTemplateId + ", count=" + count + "]";
    }

    public static final class Builder {
        private String kind;
        private @Nullable Integer slotIndex;
        private int itemTemplateId;
        private @Nullable Long count;

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public Builder slotIndex(@Nullable Integer slotIndex) {
            this.slotIndex = slotIndex;
            return this;
        }

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder count(@Nullable Long count) {
            this.count = count;
            return this;
        }

        public SpecialAbilityPrice build() {
            return new SpecialAbilityPrice(this);
        }
    }
}
