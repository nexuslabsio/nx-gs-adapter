package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.*;

/**
 * Per-module health snapshot embedded into {@link HeartbeatEvent#getEnabledModules()}.
 *
 * <p>{@code state} is a string on the wire (uppercase: {@code ACTIVE}, {@code DEGRADED},
 * {@code DISABLED}, {@code FAILED}) — keeps the platform-side consumer decoupled from
 * any JVM enum ordinal. Consumers SHOULD treat unknown values as {@code UNKNOWN} for
 * forward-compat when new states ship.</p>
 *
 * <p>{@link Stats} is a typed-slot bag for module-specific extras — {@code pool} for
 * DB-reading modules today; future slots (table list, last-sync timestamp, etc.) added
 * as additional optional fields on {@code Stats} without breaking this wire shape.</p>
 */
public final class ModuleStatus {

    private final String name;
    private final String state;
    private final Stats stats;

    public ModuleStatus(String name, String state, Stats stats) {
        this.name = name;
        this.state = state;
        this.stats = stats != null ? stats : Stats.empty();
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    public Stats getStats() {
        return stats;
    }

    public Builder toBuilder() {
        return new Builder().name(name).state(state).stats(stats);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ModuleStatus)) return false;
        ModuleStatus that = (ModuleStatus) o;
        return Objects.equals(name, that.name)
                && Objects.equals(state, that.state)
                && Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, state, stats);
    }

    @Override
    public String toString() {
        return "ModuleStatus[name=" + name + ", state=" + state + ", stats=" + stats + "]";
    }

    public static final class Builder {
        private String name;
        private String state;
        private Stats stats;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public Builder stats(Stats stats) {
            this.stats = stats;
            return this;
        }

        public ModuleStatus build() {
            return new ModuleStatus(name, state, stats);
        }
    }

    /**
     * Typed-slot bag of module-specific extras.
     * <ul>
     *     <li>{@link #getPool()} — JDBC pool counters from DB-reading modules
     *     (db-sync surfaces {@code JdbcConnectionSource.stats()}).</li>
     *     <li>{@link #getEntities()} — per-entity operational state for sync
     *     modules; populated by the CDC engine on every cycle. Replaces the
     *     earlier placeholder names-only {@code tables} list (entity-centric
     *     vocabulary; {@code "clan"}, not {@code "clan_data"}).</li>
     *     <li>{@link #getEvents()} — bounded-queue counters for the built-in
     *     {@code events} module (adapter-core's outbound fan-out).</li>
     * </ul>
     * <p>Future slots ship as additional optional fields without breaking
     * existing consumers — unknown JSON keys are ignored.</p>
     */
    public static final class Stats {

        private static final Stats EMPTY = new Stats(null, null, null);

        private final PoolStats pool;
        private final List<EntityStats> entities;
        private final EventsStats events;

        public Stats(PoolStats pool, List<EntityStats> entities) {
            this(pool, entities, null);
        }

        public Stats(PoolStats pool, List<EntityStats> entities, EventsStats events) {
            this.pool = pool;
            this.entities = entities == null
                    ? null
                    : Collections.unmodifiableList(new ArrayList<EntityStats>(entities));
            this.events = events;
        }

        public static Stats empty() {
            return EMPTY;
        }

        public Optional<PoolStats> getPool() {
            return Optional.ofNullable(pool);
        }

        public Optional<List<EntityStats>> getEntities() {
            return Optional.ofNullable(entities);
        }

        public Optional<EventsStats> getEvents() {
            return Optional.ofNullable(events);
        }

        public Builder toBuilder() {
            return new Builder().pool(pool).entities(entities).events(events);
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Stats)) return false;
            Stats that = (Stats) o;
            return Objects.equals(pool, that.pool)
                    && Objects.equals(entities, that.entities)
                    && Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pool, entities, events);
        }

        @Override
        public String toString() {
            return "Stats[pool=" + pool + ", entities=" + entities + ", events=" + events + "]";
        }

        public static final class Builder {
            private PoolStats pool;
            private List<EntityStats> entities;
            private EventsStats events;

            public Builder pool(PoolStats pool) {
                this.pool = pool;
                return this;
            }

            public Builder entities(List<EntityStats> entities) {
                this.entities = entities;
                return this;
            }

            public Builder events(EventsStats events) {
                this.events = events;
                return this;
            }

            public Stats build() {
                return new Stats(pool, entities, events);
            }
        }
    }
}
