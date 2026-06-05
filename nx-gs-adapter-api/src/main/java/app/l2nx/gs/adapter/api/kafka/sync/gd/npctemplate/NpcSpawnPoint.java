package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One vertex of a territory-based spawn polygon — a horizontal {@code (x, y)} point
 * with a vertical {@code [zmin, zmax]} band. Carried in {@link NpcSpawn#getTerritory()}
 * for area spawns; point spawns use {@link NpcSpawn}'s scalar coordinates instead.
 */
public final class NpcSpawnPoint {

    private final @Nullable Integer x;
    private final @Nullable Integer y;
    private final @Nullable Integer zmin;
    private final @Nullable Integer zmax;

    private NpcSpawnPoint(Builder b) {
        this.x = b.x;
        this.y = b.y;
        this.zmin = b.zmin;
        this.zmax = b.zmax;
    }

    public @Nullable Integer getX() {
        return x;
    }

    public @Nullable Integer getY() {
        return y;
    }

    public @Nullable Integer getZmin() {
        return zmin;
    }

    public @Nullable Integer getZmax() {
        return zmax;
    }

    public Builder toBuilder() {
        return new Builder()
                .x(x)
                .y(y)
                .zmin(zmin)
                .zmax(zmax);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcSpawnPoint)) return false;
        NpcSpawnPoint that = (NpcSpawnPoint) o;
        return Objects.equals(x, that.x)
                && Objects.equals(y, that.y)
                && Objects.equals(zmin, that.zmin)
                && Objects.equals(zmax, that.zmax);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, zmin, zmax);
    }

    @Override
    public String toString() {
        return "NpcSpawnPoint[x=" + x + ", y=" + y + ", zmin=" + zmin + ", zmax=" + zmax + "]";
    }

    public static final class Builder {
        private @Nullable Integer x;
        private @Nullable Integer y;
        private @Nullable Integer zmin;
        private @Nullable Integer zmax;

        public Builder x(@Nullable Integer x) {
            this.x = x;
            return this;
        }

        public Builder y(@Nullable Integer y) {
            this.y = y;
            return this;
        }

        public Builder zmin(@Nullable Integer zmin) {
            this.zmin = zmin;
            return this;
        }

        public Builder zmax(@Nullable Integer zmax) {
            this.zmax = zmax;
            return this;
        }

        public NpcSpawnPoint build() {
            return new NpcSpawnPoint(this);
        }
    }
}
