package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One item entry inside an NPC {@link NpcDropGroup} — the item that may drop plus its count
 * range and individual roll chance.
 *
 * <p>{@code itemTemplateId} is the non-null identity (references the item-template entity).
 * {@code min}/{@code max} are the count range ({@code Long} — adena-style stacks can exceed
 * {@code int}). {@code chancePercent} is the per-item probability in {@code [0, 100]} (the
 * provider normalizes its core's internal basis into percent).</p>
 */
public final class NpcDropItem {

    private final int itemTemplateId;
    private final @Nullable Long min;
    private final @Nullable Long max;
    private final @Nullable Double chancePercent;

    public NpcDropItem(int itemTemplateId,
                       @Nullable Long min,
                       @Nullable Long max,
                       @Nullable Double chancePercent) {
        this.itemTemplateId = itemTemplateId;
        this.min = min;
        this.max = max;
        this.chancePercent = chancePercent;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public @Nullable Long getMin() {
        return min;
    }

    public @Nullable Long getMax() {
        return max;
    }

    public @Nullable Double getChancePercent() {
        return chancePercent;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemTemplateId(itemTemplateId)
                .min(min)
                .max(max)
                .chancePercent(chancePercent);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcDropItem)) return false;
        NpcDropItem that = (NpcDropItem) o;
        return itemTemplateId == that.itemTemplateId
                && Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(chancePercent, that.chancePercent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, min, max, chancePercent);
    }

    @Override
    public String toString() {
        return "NpcDropItem[itemTemplateId=" + itemTemplateId + ", min=" + min + ", max=" + max
                + ", chancePercent=" + chancePercent + "]";
    }

    public static final class Builder {
        private int itemTemplateId;
        private @Nullable Long min;
        private @Nullable Long max;
        private @Nullable Double chancePercent;

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder min(@Nullable Long min) {
            this.min = min;
            return this;
        }

        public Builder max(@Nullable Long max) {
            this.max = max;
            return this;
        }

        public Builder chancePercent(@Nullable Double chancePercent) {
            this.chancePercent = chancePercent;
            return this;
        }

        public NpcDropItem build() {
            return new NpcDropItem(itemTemplateId, min, max, chancePercent);
        }
    }
}
