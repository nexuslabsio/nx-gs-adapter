package app.l2nx.gs.adapter.api.kafka.events.raid;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One drop entry inside a {@link RaidKillEvent} — the host's record of what
 * the raid actually rolled, not the static template drop table.
 *
 * <p>Captures only what was emitted: {@code itemId}, {@code count}, and an
 * optional {@code enchantLevel}. Drop-claim tracking (which character
 * eventually picked the item up) is intentionally out of scope for the
 * v1 wire — it would be a follow-up multi-event pivot (e.g.
 * {@code RaidDropClaimedEvent}) on the same {@code raid} family.</p>
 *
 * <p>Stack quantity is {@code long} because raid kills frequently drop adena
 * counts that overflow {@code int} (Antharas / Valakas).</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
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

    /**
     * L2 item template id of the dropped stack.
     */
    public int getItemId() {
        return itemId;
    }

    /**
     * Stack quantity. {@code >= 1} for valid entries; producers MUST NOT emit
     * a zero-count drop.
     */
    public long getCount() {
        return count;
    }

    /**
     * Enchant level of the dropped item, when the type is enchantable; {@code null}
     * for adena, materials, recipes, and anything unenchantable.
     */
    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .count(count)
                .enchantLevel(enchantLevel);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaidDropItem)) return false;
        RaidDropItem that = (RaidDropItem) o;
        return itemId == that.itemId
                && count == that.count
                && Objects.equals(enchantLevel, that.enchantLevel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, count, enchantLevel);
    }

    @Override
    public String toString() {
        return "RaidDropItem[itemId=" + itemId
                + ", count=" + count
                + ", enchantLevel=" + enchantLevel + "]";
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
