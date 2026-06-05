package app.l2nx.gs.adapter.api.kafka.sync.gd.recipetemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One material a {@link RecipeTemplate} consumes — an item reference plus the quantity
 * required. {@code itemTemplateId} is the non-null identity (the FK to the item-template
 * entity); {@code count} is how many units the craft consumes.
 */
public final class RecipeIngredient {

    private final int itemTemplateId;
    private final @Nullable Integer count;

    private RecipeIngredient(Builder b) {
        this.itemTemplateId = b.itemTemplateId;
        this.count = b.count;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Integer getCount() {
        return count;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemTemplateId(itemTemplateId)
                .count(count);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecipeIngredient)) return false;
        RecipeIngredient that = (RecipeIngredient) o;
        return itemTemplateId == that.itemTemplateId && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, count);
    }

    @Override
    public String toString() {
        return "RecipeIngredient[itemTemplateId=" + itemTemplateId + ", count=" + count + "]";
    }

    public static final class Builder {
        private int itemTemplateId;
        private @Nullable Integer count;

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder count(@Nullable Integer count) {
            this.count = count;
            return this;
        }

        public RecipeIngredient build() {
            return new RecipeIngredient(this);
        }
    }
}
