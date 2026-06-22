package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One produced item group of an extractable {@link SkillLevel} — skills that open /
 * convert an item into other items (capsules, event boxes, lottery-style extractors)
 * roll one of several groups; the rolled group grants all of its {@link SkillProducedItem}
 * entries together.
 *
 * <p>{@code chancePercent} is the group's roll chance; {@code enchantLevel} is the
 * enchant applied to the produced items ({@code null} when none). {@code items} is the
 * non-null produced bundle (≥1 entry).</p>
 */
public final class SkillProducedItemGroup {

    private final @Nullable Double chancePercent;
    private final @Nullable Integer enchantLevel;
    private final List<SkillProducedItem> items;

    public SkillProducedItemGroup(
            @Nullable Double chancePercent, @Nullable Integer enchantLevel, List<SkillProducedItem> items) {
        this.chancePercent = chancePercent;
        this.enchantLevel = enchantLevel;
        this.items =
                Collections.unmodifiableList(new ArrayList<SkillProducedItem>(Objects.requireNonNull(items, "items")));
    }

    public @Nullable Double getChancePercent() {
        return chancePercent;
    }

    /**
     * Enchant level applied to the produced items; {@code null} when none.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /**
     * Items produced together when this group is rolled.
     */
    public List<SkillProducedItem> getItems() {
        return items;
    }

    public Builder toBuilder() {
        return new Builder()
                .chancePercent(chancePercent)
                .enchantLevel(enchantLevel)
                .items(items);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillProducedItemGroup)) return false;
        SkillProducedItemGroup that = (SkillProducedItemGroup) o;
        return Objects.equals(chancePercent, that.chancePercent)
                && Objects.equals(enchantLevel, that.enchantLevel)
                && Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chancePercent, enchantLevel, items);
    }

    @Override
    public String toString() {
        return "SkillProducedItemGroup[chancePercent=" + chancePercent + ", items=" + items.size() + "]";
    }

    public static final class Builder {
        private @Nullable Double chancePercent;
        private @Nullable Integer enchantLevel;
        private List<SkillProducedItem> items;

        public Builder chancePercent(@Nullable Double chancePercent) {
            this.chancePercent = chancePercent;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder items(List<SkillProducedItem> items) {
            this.items = items;
            return this;
        }

        public SkillProducedItemGroup build() {
            return new SkillProducedItemGroup(chancePercent, enchantLevel, items);
        }
    }
}
