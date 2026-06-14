package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One item required to learn a skill at a {@link SkillClassLearn} entry — an item-template
 * reference with its required quantity. {@code itemTemplateId} is an FK to the item template
 * (not an instance); {@code itemTemplateCount} is how many are consumed.
 */
public final class SkillLearnItem {

    private final @Nullable Integer itemTemplateId;
    private final @Nullable Long itemTemplateCount;

    public SkillLearnItem(@Nullable Integer itemTemplateId,
                          @Nullable Long itemTemplateCount) {
        this.itemTemplateId = itemTemplateId;
        this.itemTemplateCount = itemTemplateCount;
    }

    /**
     * FK to the item template required to learn the skill.
     */
    public @Nullable Integer getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Required quantity of the item template.
     */
    public @Nullable Long getItemTemplateCount() {
        return itemTemplateCount;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemTemplateId(itemTemplateId)
                .itemTemplateCount(itemTemplateCount);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillLearnItem)) return false;
        SkillLearnItem that = (SkillLearnItem) o;
        return Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(itemTemplateCount, that.itemTemplateCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, itemTemplateCount);
    }

    @Override
    public String toString() {
        return "SkillLearnItem[itemTemplateId=" + itemTemplateId
                + ", itemTemplateCount=" + itemTemplateCount + "]";
    }

    public static final class Builder {
        private @Nullable Integer itemTemplateId;
        private @Nullable Long itemTemplateCount;

        public Builder itemTemplateId(@Nullable Integer itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder itemTemplateCount(@Nullable Long itemTemplateCount) {
            this.itemTemplateCount = itemTemplateCount;
            return this;
        }

        public SkillLearnItem build() {
            return new SkillLearnItem(itemTemplateId, itemTemplateCount);
        }
    }
}
