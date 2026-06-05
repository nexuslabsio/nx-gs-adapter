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
 */
public final class NpcMinionRef {

    private final int minionNpcTemplateId;
    private final @Nullable Integer count;

    private NpcMinionRef(Builder b) {
        this.minionNpcTemplateId = b.minionNpcTemplateId;
        this.count = b.count;
    }

    public int getMinionNpcTemplateId() {
        return minionNpcTemplateId;
    }

    public @Nullable Integer getCount() {
        return count;
    }

    public Builder toBuilder() {
        return new Builder()
                .minionNpcTemplateId(minionNpcTemplateId)
                .count(count);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcMinionRef)) return false;
        NpcMinionRef that = (NpcMinionRef) o;
        return minionNpcTemplateId == that.minionNpcTemplateId && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minionNpcTemplateId, count);
    }

    @Override
    public String toString() {
        return "NpcMinionRef[minionNpcTemplateId=" + minionNpcTemplateId + ", count=" + count + "]";
    }

    public static final class Builder {
        private int minionNpcTemplateId;
        private @Nullable Integer count;

        public Builder minionNpcTemplateId(int minionNpcTemplateId) {
            this.minionNpcTemplateId = minionNpcTemplateId;
            return this;
        }

        public Builder count(@Nullable Integer count) {
            this.count = count;
            return this;
        }

        public NpcMinionRef build() {
            return new NpcMinionRef(this);
        }
    }
}
