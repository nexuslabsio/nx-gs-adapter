package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Social-clan membership of an NPC — same-faction NPCs within {@code range} assist each
 * other in combat. Carried as {@link NpcTemplate#getFaction()}; the whole object is
 * omitted when the NPC belongs to no faction.
 *
 * <p>{@code name} is the host's faction identifier kept verbatim (e.g. {@code orc_clan} —
 * a free identifier, not an enum-like token); {@code range} is the assist radius in world
 * units.</p>
 */
public final class NpcFaction {

    private final @Nullable String name;
    private final @Nullable Integer range;

    public NpcFaction(@Nullable String name, @Nullable Integer range) {
        this.name = name;
        this.range = range;
    }

    public @Nullable String getName() {
        return name;
    }

    public @Nullable Integer getRange() {
        return range;
    }

    public Builder toBuilder() {
        return new Builder()
                .name(name)
                .range(range);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcFaction)) return false;
        NpcFaction that = (NpcFaction) o;
        return Objects.equals(name, that.name)
                && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, range);
    }

    @Override
    public String toString() {
        return "NpcFaction[name=" + name + ", range=" + range + "]";
    }

    public static final class Builder {
        private @Nullable String name;
        private @Nullable Integer range;

        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        public Builder range(@Nullable Integer range) {
            this.range = range;
            return this;
        }

        public NpcFaction build() {
            return new NpcFaction(name, range);
        }
    }
}
