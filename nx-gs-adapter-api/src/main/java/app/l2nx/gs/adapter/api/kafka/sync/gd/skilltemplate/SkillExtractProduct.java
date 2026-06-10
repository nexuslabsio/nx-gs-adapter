package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One product group of an extractable {@link SkillLevel} — skills that open / convert an
 * item into other items (capsules, event boxes, lottery-style extractors) roll one of
 * several product groups; the rolled group grants all of its {@link SkillExtractItem}
 * entries together.
 *
 * <p>{@code chancePercent} is the group's roll chance; {@code enchantLevel} is the
 * enchant applied to the granted items ({@code null} when none). {@code items} is the
 * non-null granted bundle (≥1 entry).</p>
 */
public final class SkillExtractProduct {

    private final @Nullable Double chancePercent;
    private final @Nullable Integer enchantLevel;
    private final List<SkillExtractItem> items;

    public SkillExtractProduct(@Nullable Double chancePercent,
                               @Nullable Integer enchantLevel,
                               List<SkillExtractItem> items) {
        this.chancePercent = chancePercent;
        this.enchantLevel = enchantLevel;
        this.items = Collections.unmodifiableList(
                new ArrayList<SkillExtractItem>(Objects.requireNonNull(items, "items")));
    }

    public @Nullable Double getChancePercent() {
        return chancePercent;
    }

    /**
     * Enchant level applied to the granted items; {@code null} when none.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    /**
     * Items granted together when this group is rolled.
     */
    public List<SkillExtractItem> getItems() {
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
        if (!(o instanceof SkillExtractProduct)) return false;
        SkillExtractProduct that = (SkillExtractProduct) o;
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
        return "SkillExtractProduct[chancePercent=" + chancePercent
                + ", items=" + items.size() + "]";
    }

    public static final class Builder {
        private @Nullable Double chancePercent;
        private @Nullable Integer enchantLevel;
        private List<SkillExtractItem> items;

        public Builder chancePercent(@Nullable Double chancePercent) {
            this.chancePercent = chancePercent;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public Builder items(List<SkillExtractItem> items) {
            this.items = items;
            return this;
        }

        public SkillExtractProduct build() {
            return new SkillExtractProduct(chancePercent, enchantLevel, items);
        }
    }
}
