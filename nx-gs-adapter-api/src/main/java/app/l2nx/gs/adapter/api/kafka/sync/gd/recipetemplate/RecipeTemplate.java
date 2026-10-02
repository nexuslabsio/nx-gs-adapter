package app.l2nx.gs.adapter.api.kafka.sync.gd.recipetemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one crafting recipe, payload of {@code GameDataSyncEvent} on the {@code recipetemplate} topic; the
 * consumer replaces ingredients atomically with the parent. Only {@link #getId()} is non-null; elsewhere {@code null} means not supplied.
 */
public final class RecipeTemplate {

    private final int id;
    private final @Nullable Integer recipeItemTemplateId;
    private final @Nullable String recipeName;
    private final @Nullable Integer craftLevel;
    private final @Nullable Boolean dwarven;
    private final @Nullable Integer successRatePercent;
    private final @Nullable Integer productItemTemplateId;
    private final @Nullable Integer productCount;
    private final @Nullable Integer rareItemTemplateId;
    private final @Nullable Integer rareCount;
    private final @Nullable Integer rarityPercent;
    private final @Nullable Integer mpConsume;
    private final @Nullable Integer hpConsume;
    private final @Nullable List<RecipeIngredient> ingredients;

    public RecipeTemplate(
            int id,
            @Nullable Integer recipeItemTemplateId,
            @Nullable String recipeName,
            @Nullable Integer craftLevel,
            @Nullable Boolean dwarven,
            @Nullable Integer successRatePercent,
            @Nullable Integer productItemTemplateId,
            @Nullable Integer productCount,
            @Nullable Integer rareItemTemplateId,
            @Nullable Integer rareCount,
            @Nullable Integer rarityPercent,
            @Nullable Integer mpConsume,
            @Nullable Integer hpConsume,
            @Nullable List<RecipeIngredient> ingredients) {
        this.id = id;
        this.recipeItemTemplateId = recipeItemTemplateId;
        this.recipeName = recipeName;
        this.craftLevel = craftLevel;
        this.dwarven = dwarven;
        this.successRatePercent = successRatePercent;
        this.productItemTemplateId = productItemTemplateId;
        this.productCount = productCount;
        this.rareItemTemplateId = rareItemTemplateId;
        this.rareCount = rareCount;
        this.rarityPercent = rarityPercent;
        this.mpConsume = mpConsume;
        this.hpConsume = hpConsume;
        this.ingredients =
                ingredients == null ? null : Collections.unmodifiableList(new ArrayList<RecipeIngredient>(ingredients));
    }

    public int getId() {
        return id;
    }

    public @Nullable Integer getRecipeItemTemplateId() {
        return recipeItemTemplateId;
    }

    /**
     * Internal code, not localized.
     */
    public @Nullable String getRecipeName() {
        return recipeName;
    }

    public @Nullable Integer getCraftLevel() {
        return craftLevel;
    }

    public @Nullable Boolean getDwarven() {
        return dwarven;
    }

    public @Nullable Integer getSuccessRatePercent() {
        return successRatePercent;
    }

    public @Nullable Integer getProductItemTemplateId() {
        return productItemTemplateId;
    }

    public @Nullable Integer getProductCount() {
        return productCount;
    }

    /**
     * Masterwork product, replaces the normal one with {@link #getRarityPercent()} chance.
     */
    public @Nullable Integer getRareItemTemplateId() {
        return rareItemTemplateId;
    }

    public @Nullable Integer getRareCount() {
        return rareCount;
    }

    public @Nullable Integer getRarityPercent() {
        return rarityPercent;
    }

    public @Nullable Integer getMpConsume() {
        return mpConsume;
    }

    public @Nullable Integer getHpConsume() {
        return hpConsume;
    }

    public @Nullable List<RecipeIngredient> getIngredients() {
        return ingredients;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .recipeItemTemplateId(recipeItemTemplateId)
                .recipeName(recipeName)
                .craftLevel(craftLevel)
                .dwarven(dwarven)
                .successRatePercent(successRatePercent)
                .productItemTemplateId(productItemTemplateId)
                .productCount(productCount)
                .rareItemTemplateId(rareItemTemplateId)
                .rareCount(rareCount)
                .rarityPercent(rarityPercent)
                .mpConsume(mpConsume)
                .hpConsume(hpConsume)
                .ingredients(ingredients);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecipeTemplate)) return false;
        RecipeTemplate that = (RecipeTemplate) o;
        return id == that.id
                && Objects.equals(recipeItemTemplateId, that.recipeItemTemplateId)
                && Objects.equals(recipeName, that.recipeName)
                && Objects.equals(craftLevel, that.craftLevel)
                && Objects.equals(dwarven, that.dwarven)
                && Objects.equals(successRatePercent, that.successRatePercent)
                && Objects.equals(productItemTemplateId, that.productItemTemplateId)
                && Objects.equals(productCount, that.productCount)
                && Objects.equals(rareItemTemplateId, that.rareItemTemplateId)
                && Objects.equals(rareCount, that.rareCount)
                && Objects.equals(rarityPercent, that.rarityPercent)
                && Objects.equals(mpConsume, that.mpConsume)
                && Objects.equals(hpConsume, that.hpConsume)
                && Objects.equals(ingredients, that.ingredients);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                recipeItemTemplateId,
                recipeName,
                craftLevel,
                dwarven,
                successRatePercent,
                productItemTemplateId,
                productCount,
                rareItemTemplateId,
                rareCount,
                rarityPercent,
                mpConsume,
                hpConsume,
                ingredients);
    }

    @Override
    public String toString() {
        return "RecipeTemplate[id=" + id + ", productItemTemplateId=" + productItemTemplateId + ", craftLevel="
                + craftLevel + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable Integer recipeItemTemplateId;
        private @Nullable String recipeName;
        private @Nullable Integer craftLevel;
        private @Nullable Boolean dwarven;
        private @Nullable Integer successRatePercent;
        private @Nullable Integer productItemTemplateId;
        private @Nullable Integer productCount;
        private @Nullable Integer rareItemTemplateId;
        private @Nullable Integer rareCount;
        private @Nullable Integer rarityPercent;
        private @Nullable Integer mpConsume;
        private @Nullable Integer hpConsume;
        private @Nullable List<RecipeIngredient> ingredients;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder recipeItemTemplateId(@Nullable Integer recipeItemTemplateId) {
            this.recipeItemTemplateId = recipeItemTemplateId;
            return this;
        }

        public Builder recipeName(@Nullable String recipeName) {
            this.recipeName = recipeName;
            return this;
        }

        public Builder craftLevel(@Nullable Integer craftLevel) {
            this.craftLevel = craftLevel;
            return this;
        }

        public Builder dwarven(@Nullable Boolean dwarven) {
            this.dwarven = dwarven;
            return this;
        }

        public Builder successRatePercent(@Nullable Integer successRatePercent) {
            this.successRatePercent = successRatePercent;
            return this;
        }

        public Builder productItemTemplateId(@Nullable Integer productItemTemplateId) {
            this.productItemTemplateId = productItemTemplateId;
            return this;
        }

        public Builder productCount(@Nullable Integer productCount) {
            this.productCount = productCount;
            return this;
        }

        public Builder rareItemTemplateId(@Nullable Integer rareItemTemplateId) {
            this.rareItemTemplateId = rareItemTemplateId;
            return this;
        }

        public Builder rareCount(@Nullable Integer rareCount) {
            this.rareCount = rareCount;
            return this;
        }

        public Builder rarityPercent(@Nullable Integer rarityPercent) {
            this.rarityPercent = rarityPercent;
            return this;
        }

        public Builder mpConsume(@Nullable Integer mpConsume) {
            this.mpConsume = mpConsume;
            return this;
        }

        public Builder hpConsume(@Nullable Integer hpConsume) {
            this.hpConsume = hpConsume;
            return this;
        }

        public Builder ingredients(@Nullable List<RecipeIngredient> ingredients) {
            this.ingredients = ingredients;
            return this;
        }

        public RecipeTemplate build() {
            return new RecipeTemplate(
                    id,
                    recipeItemTemplateId,
                    recipeName,
                    craftLevel,
                    dwarven,
                    successRatePercent,
                    productItemTemplateId,
                    productCount,
                    rareItemTemplateId,
                    rareCount,
                    rarityPercent,
                    mpConsume,
                    hpConsume,
                    ingredients);
        }
    }
}
