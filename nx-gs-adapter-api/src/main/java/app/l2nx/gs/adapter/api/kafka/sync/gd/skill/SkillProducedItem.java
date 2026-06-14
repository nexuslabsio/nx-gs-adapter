package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One produced item of a {@link SkillProducedItemGroup} — an item-template reference
 * with its quantity range. {@code itemTemplateId} is the non-null identity.
 * {@code minCount} / {@code maxCount} bound the produced quantity; a fixed quantity
 * carries {@code minCount} only ({@code maxCount} {@code null}).
 */
public final class SkillProducedItem {

    private final int itemTemplateId;
    private final @Nullable Long minCount;
    private final @Nullable Long maxCount;

    public SkillProducedItem(int itemTemplateId,
                             @Nullable Long minCount,
                             @Nullable Long maxCount) {
        this.itemTemplateId = itemTemplateId;
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Long getMinCount() {
        return minCount;
    }

    /**
     * Upper bound of the produced quantity; {@code null} when the quantity is fixed at
     * {@code minCount}.
     */
    public @Nullable Long getMaxCount() {
        return maxCount;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemTemplateId(itemTemplateId)
                .minCount(minCount)
                .maxCount(maxCount);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillProducedItem)) return false;
        SkillProducedItem that = (SkillProducedItem) o;
        return itemTemplateId == that.itemTemplateId
                && Objects.equals(minCount, that.minCount)
                && Objects.equals(maxCount, that.maxCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, minCount, maxCount);
    }

    @Override
    public String toString() {
        return "SkillProducedItem[itemTemplateId=" + itemTemplateId
                + ", minCount=" + minCount + ", maxCount=" + maxCount + "]";
    }

    public static final class Builder {
        private int itemTemplateId;
        private @Nullable Long minCount;
        private @Nullable Long maxCount;

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder minCount(@Nullable Long minCount) {
            this.minCount = minCount;
            return this;
        }

        public Builder maxCount(@Nullable Long maxCount) {
            this.maxCount = maxCount;
            return this;
        }

        public SkillProducedItem build() {
            return new SkillProducedItem(itemTemplateId, minCount, maxCount);
        }
    }
}
