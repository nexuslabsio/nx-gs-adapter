package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * A reference to a skill granted by an item — the intrinsic {@code (id, level)} the
 * item's datapack template carries, plus how it is granted. The full skill (name /
 * description / effects) lives in the skills entity; this is only the cross-reference,
 * so it can ship before that entity exists.
 *
 * <p>{@code id}+{@code level} are the non-null identity of the reference. {@code type}
 * is an open, nullable string (build-agnostic vocabulary): {@code NORMAL},
 * {@code ENCHANT}, {@code EQUIP}, {@code UNEQUIP}, {@code CRITICAL_SKILL},
 * {@code MAGIC_SKILL}. {@code chance} is the proc chance (%), present for conditional
 * grants (on-crit / on-magic), {@code null} otherwise.</p>
 */
public final class ItemSkillRef {

    private final int id;
    private final int level;
    private final @Nullable String type;
    private final @Nullable Integer chance;

    private ItemSkillRef(Builder b) {
        this.id = b.id;
        this.level = b.level;
        this.type = b.type;
        this.chance = b.chance;
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

    public @Nullable Integer getChance() {
        return chance;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .level(level)
                .type(type)
                .chance(chance);
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
                && Objects.equals(chance, that.chance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, level, type, chance);
    }

    @Override
    public String toString() {
        return "ItemSkillRef[id=" + id + ", level=" + level + ", type=" + type + ", chance=" + chance + "]";
    }

    public static final class Builder {
        private int id;
        private int level;
        private @Nullable String type;
        private @Nullable Integer chance;

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

        public Builder chance(@Nullable Integer chance) {
            this.chance = chance;
            return this;
        }

        public ItemSkillRef build() {
            return new ItemSkillRef(this);
        }
    }
}
