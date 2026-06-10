package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * A leader→minion relationship of an NPC — the minion's template id and how many
 * spawn with the leader. The leader-side "spawn random minions" flag is carried on
 * {@link NpcTemplate#getRandomMinions()} since it applies to the leader, not the pair.
 *
 * <p>{@code minionNpcTemplateId} is the non-null identity. The minion template may not exist
 * in the snapshot's NPC set (a minion can be defined in an unloaded file), so no
 * referential guarantee is implied.</p>
 *
 * <p>{@code groupIndex} identifies the alternative minion set this ref belongs to when the
 * leader spawns one of several random sets (see {@link NpcTemplate#getRandomMinions()}) —
 * refs sharing a {@code groupIndex} spawn together, distinct indices are mutually exclusive
 * alternatives. {@code null} when the leader has a single fixed set. Without it, random sets
 * built from the same minion ids would collapse into duplicate {@code (leader, minion)} pairs.</p>
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
        return "NpcMinionRef[minionNpcTemplateId=" + minionNpcTemplateId
                + ", count=" + count + ", groupIndex=" + groupIndex + "]";
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
