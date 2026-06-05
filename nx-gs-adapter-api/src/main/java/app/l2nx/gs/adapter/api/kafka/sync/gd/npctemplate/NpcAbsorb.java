package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import app.l2nx.gs.adapter.api.domain.npc.NpcAbsorbType;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One soul-absorb rule of an NPC — who may absorb, within which player-level band, at what
 * chance. Carried in {@link NpcTemplate#getAbsorbs()}.
 *
 * <p>All fields {@link Nullable}: {@code type} is the absorb mode, {@code minLevel}/{@code maxLevel}
 * the inclusive player-level band, {@code chancePercent} the base probability and
 * {@code cursedChancePercent} the cursed-weapon variant.</p>
 */
public final class NpcAbsorb {

    private final @Nullable NpcAbsorbType type;
    private final @Nullable Integer minLevel;
    private final @Nullable Integer maxLevel;
    private final @Nullable Integer chancePercent;
    private final @Nullable Integer cursedChancePercent;

    public NpcAbsorb(@Nullable NpcAbsorbType type,
                     @Nullable Integer minLevel,
                     @Nullable Integer maxLevel,
                     @Nullable Integer chancePercent,
                     @Nullable Integer cursedChancePercent) {
        this.type = type;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.chancePercent = chancePercent;
        this.cursedChancePercent = cursedChancePercent;
    }

    public @Nullable NpcAbsorbType getType() {
        return type;
    }

    public @Nullable Integer getMinLevel() {
        return minLevel;
    }

    public @Nullable Integer getMaxLevel() {
        return maxLevel;
    }

    public @Nullable Integer getChancePercent() {
        return chancePercent;
    }

    public @Nullable Integer getCursedChancePercent() {
        return cursedChancePercent;
    }

    public Builder toBuilder() {
        return new Builder()
                .type(type)
                .minLevel(minLevel)
                .maxLevel(maxLevel)
                .chancePercent(chancePercent)
                .cursedChancePercent(cursedChancePercent);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcAbsorb)) return false;
        NpcAbsorb that = (NpcAbsorb) o;
        return type == that.type
                && Objects.equals(minLevel, that.minLevel)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(chancePercent, that.chancePercent)
                && Objects.equals(cursedChancePercent, that.cursedChancePercent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, minLevel, maxLevel, chancePercent, cursedChancePercent);
    }

    @Override
    public String toString() {
        return "NpcAbsorb[type=" + type + ", minLevel=" + minLevel + ", maxLevel=" + maxLevel + "]";
    }

    public static final class Builder {
        private @Nullable NpcAbsorbType type;
        private @Nullable Integer minLevel;
        private @Nullable Integer maxLevel;
        private @Nullable Integer chancePercent;
        private @Nullable Integer cursedChancePercent;

        public Builder type(@Nullable NpcAbsorbType type) {
            this.type = type;
            return this;
        }

        public Builder minLevel(@Nullable Integer minLevel) {
            this.minLevel = minLevel;
            return this;
        }

        public Builder maxLevel(@Nullable Integer maxLevel) {
            this.maxLevel = maxLevel;
            return this;
        }

        public Builder chancePercent(@Nullable Integer chancePercent) {
            this.chancePercent = chancePercent;
            return this;
        }

        public Builder cursedChancePercent(@Nullable Integer cursedChancePercent) {
            this.cursedChancePercent = cursedChancePercent;
            return this;
        }

        public NpcAbsorb build() {
            return new NpcAbsorb(type, minLevel, maxLevel, chancePercent, cursedChancePercent);
        }
    }
}
