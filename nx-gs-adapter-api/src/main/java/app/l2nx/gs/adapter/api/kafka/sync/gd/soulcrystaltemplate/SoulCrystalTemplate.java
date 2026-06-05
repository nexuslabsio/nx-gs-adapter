package app.l2nx.gs.adapter.api.kafka.sync.gd.soulcrystaltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Build-agnostic soul-crystal wire DTO — one node of the soul-crystal leveling chain,
 * carried as the payload of {@code GameDataSyncEvent} on the {@code gd} (game-data) sync
 * stream's {@code soulcrystaltemplate} entity topic. A soul crystal is an item that levels
 * up; {@link #getNextItemTemplateId()} points at the next-level crystal and
 * {@link #getCursedNextItemTemplateId()} at the cursed variant after a failed level-up.
 *
 * <p>Flat aggregate (no children) keyed by the crystal's own item id. Item references use
 * the canonical {@code itemTemplateId} name. Only {@link #getId()} is non-null.</p>
 */
public final class SoulCrystalTemplate {

    private final int id;
    private final @Nullable Integer level;
    private final @Nullable Integer nextItemTemplateId;
    private final @Nullable Integer cursedNextItemTemplateId;

    public SoulCrystalTemplate(int id,
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

    /**
     * Crystal level within the leveling chain.
     */
    public @Nullable Integer getLevel() {
        return level;
    }

    /**
     * Next-level crystal item this one upgrades into; {@code null} at the top of the chain.
     */
    public @Nullable Integer getNextItemTemplateId() {
        return nextItemTemplateId;
    }

    /**
     * Cursed crystal item produced on a failed level-up; {@code null} if not applicable.
     */
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
