package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One spawn definition for an NPC — where / how many / how often it appears. Carried in
 * {@link NpcTemplate#getSpawns()} (the provider attaches each spawn to its NPC, joining the
 * host's spawn registry to the template set).
 *
 * <p>A spawn is either point-based (scalar {@code x}/{@code y}/{@code z}/{@code heading}) or
 * area-based ({@link #getTerritory()} polygon); a given spawn uses one or the other. The actual
 * respawn time is random in {@code [respawnSec - respawnRandomSec, respawnSec +
 * respawnRandomSec]}. {@code periodOfDay} (e.g. {@code day}/{@code night}) and
 * {@code respawnPattern} (cron-like schedule) are open strings. All fields {@link Nullable}.</p>
 */
public final class NpcSpawn {

    private final @Nullable Integer x;
    private final @Nullable Integer y;
    private final @Nullable Integer z;
    private final @Nullable Integer heading;
    private final @Nullable Integer count;
    private final @Nullable Integer respawnSec;
    private final @Nullable Integer respawnRandomSec;
    private final @Nullable String periodOfDay;
    private final @Nullable String respawnPattern;
    private final @Nullable List<NpcSpawnPoint> territory;

    private NpcSpawn(Builder b) {
        this.x = b.x;
        this.y = b.y;
        this.z = b.z;
        this.heading = b.heading;
        this.count = b.count;
        this.respawnSec = b.respawnSec;
        this.respawnRandomSec = b.respawnRandomSec;
        this.periodOfDay = b.periodOfDay;
        this.respawnPattern = b.respawnPattern;
        this.territory = b.territory == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcSpawnPoint>(b.territory));
    }

    public @Nullable Integer getX() {
        return x;
    }

    public @Nullable Integer getY() {
        return y;
    }

    public @Nullable Integer getZ() {
        return z;
    }

    public @Nullable Integer getHeading() {
        return heading;
    }

    public @Nullable Integer getCount() {
        return count;
    }

    public @Nullable Integer getRespawnSec() {
        return respawnSec;
    }

    public @Nullable Integer getRespawnRandomSec() {
        return respawnRandomSec;
    }

    public @Nullable String getPeriodOfDay() {
        return periodOfDay;
    }

    public @Nullable String getRespawnPattern() {
        return respawnPattern;
    }

    public @Nullable List<NpcSpawnPoint> getTerritory() {
        return territory;
    }

    public Builder toBuilder() {
        return new Builder()
                .x(x)
                .y(y)
                .z(z)
                .heading(heading)
                .count(count)
                .respawnSec(respawnSec)
                .respawnRandomSec(respawnRandomSec)
                .periodOfDay(periodOfDay)
                .respawnPattern(respawnPattern)
                .territory(territory);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcSpawn)) return false;
        NpcSpawn that = (NpcSpawn) o;
        return Objects.equals(x, that.x)
                && Objects.equals(y, that.y)
                && Objects.equals(z, that.z)
                && Objects.equals(heading, that.heading)
                && Objects.equals(count, that.count)
                && Objects.equals(respawnSec, that.respawnSec)
                && Objects.equals(respawnRandomSec, that.respawnRandomSec)
                && Objects.equals(periodOfDay, that.periodOfDay)
                && Objects.equals(respawnPattern, that.respawnPattern)
                && Objects.equals(territory, that.territory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, z, heading, count, respawnSec, respawnRandomSec, periodOfDay,
                respawnPattern, territory);
    }

    @Override
    public String toString() {
        return "NpcSpawn[x=" + x + ", y=" + y + ", z=" + z + ", count=" + count + "]";
    }

    public static final class Builder {
        private @Nullable Integer x;
        private @Nullable Integer y;
        private @Nullable Integer z;
        private @Nullable Integer heading;
        private @Nullable Integer count;
        private @Nullable Integer respawnSec;
        private @Nullable Integer respawnRandomSec;
        private @Nullable String periodOfDay;
        private @Nullable String respawnPattern;
        private @Nullable List<NpcSpawnPoint> territory;

        public Builder x(@Nullable Integer x) {
            this.x = x;
            return this;
        }

        public Builder y(@Nullable Integer y) {
            this.y = y;
            return this;
        }

        public Builder z(@Nullable Integer z) {
            this.z = z;
            return this;
        }

        public Builder heading(@Nullable Integer heading) {
            this.heading = heading;
            return this;
        }

        public Builder count(@Nullable Integer count) {
            this.count = count;
            return this;
        }

        public Builder respawnSec(@Nullable Integer respawnSec) {
            this.respawnSec = respawnSec;
            return this;
        }

        public Builder respawnRandomSec(@Nullable Integer respawnRandomSec) {
            this.respawnRandomSec = respawnRandomSec;
            return this;
        }

        public Builder periodOfDay(@Nullable String periodOfDay) {
            this.periodOfDay = periodOfDay;
            return this;
        }

        public Builder respawnPattern(@Nullable String respawnPattern) {
            this.respawnPattern = respawnPattern;
            return this;
        }

        public Builder territory(@Nullable List<NpcSpawnPoint> territory) {
            this.territory = territory;
            return this;
        }

        public NpcSpawn build() {
            return new NpcSpawn(this);
        }
    }
}
