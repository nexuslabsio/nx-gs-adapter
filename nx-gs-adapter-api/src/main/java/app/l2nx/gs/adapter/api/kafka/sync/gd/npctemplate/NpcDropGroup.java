package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import app.l2nx.gs.adapter.api.domain.npc.NpcDropType;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One reward group of an NPC's drop list — a category, a group-selection chance, and the items
 * that may drop once the group is selected. Carried in {@link NpcTemplate#getDrops()}.
 *
 * <p>{@code groupChancePercent} is the probability the group is chosen ({@code [0, 100]}); each
 * {@link NpcDropItem} then rolls its own {@code chancePercent}. {@code groupIndex} orders groups
 * within the NPC's list. All fields {@link Nullable}.</p>
 */
public final class NpcDropGroup {

    private final @Nullable NpcDropType category;
    private final @Nullable Integer groupIndex;
    private final @Nullable Double groupChancePercent;
    private final @Nullable List<NpcDropItem> items;

    private NpcDropGroup(Builder b) {
        this.category = b.category;
        this.groupIndex = b.groupIndex;
        this.groupChancePercent = b.groupChancePercent;
        this.items = b.items == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcDropItem>(b.items));
    }

    public @Nullable NpcDropType getCategory() {
        return category;
    }

    public @Nullable Integer getGroupIndex() {
        return groupIndex;
    }

    public @Nullable Double getGroupChancePercent() {
        return groupChancePercent;
    }

    public @Nullable List<NpcDropItem> getItems() {
        return items;
    }

    public Builder toBuilder() {
        return new Builder()
                .category(category)
                .groupIndex(groupIndex)
                .groupChancePercent(groupChancePercent)
                .items(items);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcDropGroup)) return false;
        NpcDropGroup that = (NpcDropGroup) o;
        return category == that.category
                && Objects.equals(groupIndex, that.groupIndex)
                && Objects.equals(groupChancePercent, that.groupChancePercent)
                && Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, groupIndex, groupChancePercent, items);
    }

    @Override
    public String toString() {
        return "NpcDropGroup[category=" + category + ", groupIndex=" + groupIndex
                + ", groupChancePercent=" + groupChancePercent + "]";
    }

    public static final class Builder {
        private @Nullable NpcDropType category;
        private @Nullable Integer groupIndex;
        private @Nullable Double groupChancePercent;
        private @Nullable List<NpcDropItem> items;

        public Builder category(@Nullable NpcDropType category) {
            this.category = category;
            return this;
        }

        public Builder groupIndex(@Nullable Integer groupIndex) {
            this.groupIndex = groupIndex;
            return this;
        }

        public Builder groupChancePercent(@Nullable Double groupChancePercent) {
            this.groupChancePercent = groupChancePercent;
            return this;
        }

        public Builder items(@Nullable List<NpcDropItem> items) {
            this.items = items;
            return this;
        }

        public NpcDropGroup build() {
            return new NpcDropGroup(this);
        }
    }
}
