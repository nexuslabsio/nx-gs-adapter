package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One granted item of a {@link SkillExtractProduct} — an item-template reference with its
 * quantity range. {@code itemTemplateId} is the non-null identity. {@code minCount} /
 * {@code maxCount} bound the granted quantity; a fixed quantity carries
 * {@code minCount} only ({@code maxCount} {@code null}).
 */
public final class SkillExtractItem {

    private final int itemTemplateId;
    private final @Nullable Long minCount;
    private final @Nullable Long maxCount;

    public SkillExtractItem(int itemTemplateId,
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
     * Upper bound of the granted quantity; {@code null} when the quantity is fixed at
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
        if (!(o instanceof SkillExtractItem)) return false;
        SkillExtractItem that = (SkillExtractItem) o;
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
        return "SkillExtractItem[itemTemplateId=" + itemTemplateId
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

        public SkillExtractItem build() {
            return new SkillExtractItem(itemTemplateId, minCount, maxCount);
        }
    }
}
