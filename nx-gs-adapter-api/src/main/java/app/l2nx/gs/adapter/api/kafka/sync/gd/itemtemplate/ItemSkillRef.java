package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A reference to a skill granted by an item — the intrinsic {@code (id, level)} the
 * item's datapack template carries, plus how it is granted. The full skill (name /
 * description / effects) lives in the skills entity; this is only the cross-reference,
 * so it can ship before that entity exists.
 *
 * <p>{@code id}+{@code level} are the non-null identity of the reference. {@code type}
 * is an open, nullable string (build-agnostic vocabulary): {@code NORMAL},
 * {@code ENCHANT}, {@code EQUIP}, {@code UNEQUIP}, {@code CRITICAL_SKILL},
 * {@code MAGIC_SKILL}. {@code chancePercent} is the proc chance ({@code [0, 100]}), present
 * for conditional grants (on-crit / on-magic), {@code null} otherwise.</p>
 */
public final class ItemSkillRef {

    private final int id;
    private final int level;
    private final @Nullable String type;
    private final @Nullable Integer chancePercent;

    public ItemSkillRef(int id, int level, @Nullable String type, @Nullable Integer chancePercent) {
        this.id = id;
        this.level = level;
        this.type = type;
        this.chancePercent = chancePercent;
    }

    public int getId() {
        return id;
    }

    public int getLevel() {
        return level;
    }

    public @Nullable String getType() {
        return type;
    }

    public @Nullable Integer getChancePercent() {
        return chancePercent;
    }

    public Builder toBuilder() {
        return new Builder().id(id).level(level).type(type).chancePercent(chancePercent);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemSkillRef)) return false;
        ItemSkillRef that = (ItemSkillRef) o;
        return id == that.id
                && level == that.level
                && Objects.equals(type, that.type)
                && Objects.equals(chancePercent, that.chancePercent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, level, type, chancePercent);
    }

    @Override
    public String toString() {
        return "ItemSkillRef[id=" + id + ", level=" + level + ", type=" + type + ", chancePercent=" + chancePercent
                + "]";
    }

    public static final class Builder {
        private int id;
        private int level;
        private @Nullable String type;
        private @Nullable Integer chancePercent;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public Builder type(@Nullable String type) {
            this.type = type;
            return this;
        }

        public Builder chancePercent(@Nullable Integer chancePercent) {
            this.chancePercent = chancePercent;
            return this;
        }

        public ItemSkillRef build() {
            return new ItemSkillRef(id, level, type, chancePercent);
        }
    }
}
