package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One drop entry of a {@link RaidKillEvent}: what the raid actually rolled, not the template drop table.
 * Drop-claim tracking is out of scope.
 */
public final class RaidDropItem {

    private final int itemId;
    private final long count;
    private final @Nullable Integer enchantLevel;

    public RaidDropItem(int itemId, long count, @Nullable Integer enchantLevel) {
        this.itemId = itemId;
        this.count = count;
        this.enchantLevel = enchantLevel;
    }

    public int getItemId() {
        return itemId;
    }

    /** {@code >= 1}; producers MUST NOT emit a zero-count drop. {@code long} because adena stacks overflow {@code int}. */
    public long getCount() {
        return count;
    }

    /** {@code null} for unenchantable items (adena, materials, recipes). */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public Builder toBuilder() {
        return new Builder().itemId(itemId).count(count).enchantLevel(enchantLevel);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaidDropItem)) return false;
        RaidDropItem that = (RaidDropItem) o;
        return itemId == that.itemId && count == that.count && Objects.equals(enchantLevel, that.enchantLevel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, count, enchantLevel);
    }

    @Override
    public String toString() {
        return "RaidDropItem[itemId=" + itemId + ", count=" + count + ", enchantLevel=" + enchantLevel + "]";
    }

    public static final class Builder {
        private int itemId;
        private long count;
        private @Nullable Integer enchantLevel;

        public Builder itemId(int itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder count(long count) {
            this.count = count;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public RaidDropItem build() {
            return new RaidDropItem(itemId, count, enchantLevel);
        }
    }
}
