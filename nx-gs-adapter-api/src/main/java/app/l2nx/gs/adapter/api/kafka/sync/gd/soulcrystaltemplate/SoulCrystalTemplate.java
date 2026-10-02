package app.l2nx.gs.adapter.api.kafka.sync.gd.soulcrystaltemplate;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One node of the soul-crystal leveling chain; the next-level crystal is {@code null} at the top, and the cursed
 * variant is what a failed level-up yields.
 */
public final class SoulCrystalTemplate {

    private final int id;
    private final @Nullable Integer level;
    private final @Nullable Integer nextItemTemplateId;
    private final @Nullable Integer cursedNextItemTemplateId;

    public SoulCrystalTemplate(
            int id,
            @Nullable Integer level,
            @Nullable Integer nextItemTemplateId,
            @Nullable Integer cursedNextItemTemplateId) {
        this.id = id;
        this.level = level;
        this.nextItemTemplateId = nextItemTemplateId;
        this.cursedNextItemTemplateId = cursedNextItemTemplateId;
    }

    public int getId() {
        return id;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    public @Nullable Integer getNextItemTemplateId() {
        return nextItemTemplateId;
    }

    public @Nullable Integer getCursedNextItemTemplateId() {
        return cursedNextItemTemplateId;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .level(level)
                .nextItemTemplateId(nextItemTemplateId)
                .cursedNextItemTemplateId(cursedNextItemTemplateId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SoulCrystalTemplate)) return false;
        SoulCrystalTemplate that = (SoulCrystalTemplate) o;
        return id == that.id
                && Objects.equals(level, that.level)
                && Objects.equals(nextItemTemplateId, that.nextItemTemplateId)
                && Objects.equals(cursedNextItemTemplateId, that.cursedNextItemTemplateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, level, nextItemTemplateId, cursedNextItemTemplateId);
    }

    @Override
    public String toString() {
        return "SoulCrystalTemplate[id=" + id + ", level=" + level + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable Integer level;
        private @Nullable Integer nextItemTemplateId;
        private @Nullable Integer cursedNextItemTemplateId;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        public Builder nextItemTemplateId(@Nullable Integer nextItemTemplateId) {
            this.nextItemTemplateId = nextItemTemplateId;
            return this;
        }

        public Builder cursedNextItemTemplateId(@Nullable Integer cursedNextItemTemplateId) {
            this.cursedNextItemTemplateId = cursedNextItemTemplateId;
            return this;
        }

        public SoulCrystalTemplate build() {
            return new SoulCrystalTemplate(id, level, nextItemTemplateId, cursedNextItemTemplateId);
        }
    }
}
