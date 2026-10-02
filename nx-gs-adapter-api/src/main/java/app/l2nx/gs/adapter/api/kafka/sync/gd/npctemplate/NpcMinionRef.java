package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The minion template may be absent from the snapshot's NPC set, so no referential guarantee. Refs sharing a
 * {@code groupIndex} spawn together; distinct indices are mutually exclusive random sets, {@code null} for one fixed set.
 */
public final class NpcMinionRef {

    private final int minionNpcTemplateId;
    private final @Nullable Integer count;
    private final @Nullable Integer groupIndex;

    public NpcMinionRef(int minionNpcTemplateId, @Nullable Integer count, @Nullable Integer groupIndex) {
        this.minionNpcTemplateId = minionNpcTemplateId;
        this.count = count;
        this.groupIndex = groupIndex;
    }

    public int getMinionNpcTemplateId() {
        return minionNpcTemplateId;
    }

    public @Nullable Integer getCount() {
        return count;
    }

    public @Nullable Integer getGroupIndex() {
        return groupIndex;
    }

    public Builder toBuilder() {
        return new Builder()
                .minionNpcTemplateId(minionNpcTemplateId)
                .count(count)
                .groupIndex(groupIndex);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcMinionRef)) return false;
        NpcMinionRef that = (NpcMinionRef) o;
        return minionNpcTemplateId == that.minionNpcTemplateId
                && Objects.equals(count, that.count)
                && Objects.equals(groupIndex, that.groupIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minionNpcTemplateId, count, groupIndex);
    }

    @Override
    public String toString() {
        return "NpcMinionRef[minionNpcTemplateId=" + minionNpcTemplateId + ", count=" + count + ", groupIndex="
                + groupIndex + "]";
    }

    public static final class Builder {
        private int minionNpcTemplateId;
        private @Nullable Integer count;
        private @Nullable Integer groupIndex;

        public Builder minionNpcTemplateId(int minionNpcTemplateId) {
            this.minionNpcTemplateId = minionNpcTemplateId;
            return this;
        }

        public Builder count(@Nullable Integer count) {
            this.count = count;
            return this;
        }

        public Builder groupIndex(@Nullable Integer groupIndex) {
            this.groupIndex = groupIndex;
            return this;
        }

        public NpcMinionRef build() {
            return new NpcMinionRef(minionNpcTemplateId, count, groupIndex);
        }
    }
}
