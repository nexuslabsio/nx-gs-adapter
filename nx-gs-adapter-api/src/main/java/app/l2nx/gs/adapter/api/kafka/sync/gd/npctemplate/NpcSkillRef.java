package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import java.util.Objects;

/**
 * Cross-reference by {@code (id, level)}; the full skill lives in the skills entity. The race-marker skill is not
 * emitted, it derives {@link NpcTemplate#getRace()}.
 */
public final class NpcSkillRef {

    private final int id;
    private final int level;

    public NpcSkillRef(int id, int level) {
        this.id = id;
        this.level = level;
    }

    public int getId() {
        return id;
    }

    public int getLevel() {
        return level;
    }

    public Builder toBuilder() {
        return new Builder().id(id).level(level);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcSkillRef)) return false;
        NpcSkillRef that = (NpcSkillRef) o;
        return id == that.id && level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, level);
    }

    @Override
    public String toString() {
        return "NpcSkillRef[id=" + id + ", level=" + level + "]";
    }

    public static final class Builder {
        private int id;
        private int level;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public NpcSkillRef build() {
            return new NpcSkillRef(id, level);
        }
    }
}
