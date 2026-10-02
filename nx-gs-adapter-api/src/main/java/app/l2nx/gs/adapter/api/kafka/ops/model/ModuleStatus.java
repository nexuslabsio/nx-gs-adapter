package app.l2nx.gs.adapter.api.kafka.ops.model;

import app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent;
import java.util.*;

/**
 * Per-module health snapshot embedded in {@link HeartbeatEvent#getEnabledModules()}.
 * {@code state} is an uppercase string ({@code ACTIVE}, {@code DEGRADED}, {@code DISABLED}, {@code FAILED}); consumers SHOULD treat unknown values as {@code UNKNOWN}.
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
     * Module-specific extras; each slot is optional and unknown JSON keys are ignored, so new slots are non-breaking.
     * {@code entities} uses entity names ({@code "clan"}), not table names.
     */
    public static final class Stats {

        private static final Stats EMPTY = new Stats(null, null, null, null);

        private final PoolStats pool;
        private final List<EntityStats> entities;
        private final EventsStats events;
        private final CommandsStats commands;

        public Stats(PoolStats pool, List<EntityStats> entities) {
            this(pool, entities, null, null);
        }

        public Stats(PoolStats pool, List<EntityStats> entities, EventsStats events) {
            this(pool, entities, events, null);
        }

        public Stats(PoolStats pool, List<EntityStats> entities, EventsStats events, CommandsStats commands) {
            this.pool = pool;
            this.entities =
                    entities == null ? null : Collections.unmodifiableList(new ArrayList<EntityStats>(entities));
            this.events = events;
            this.commands = commands;
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

        public Optional<CommandsStats> getCommands() {
            return Optional.ofNullable(commands);
        }

        public Builder toBuilder() {
            return new Builder().pool(pool).entities(entities).events(events).commands(commands);
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
                    && Objects.equals(events, that.events)
                    && Objects.equals(commands, that.commands);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pool, entities, events, commands);
        }

        @Override
        public String toString() {
            return "Stats[pool=" + pool + ", entities=" + entities + ", events=" + events + ", commands=" + commands
                    + "]";
        }

        public static final class Builder {
            private PoolStats pool;
            private List<EntityStats> entities;
            private EventsStats events;
            private CommandsStats commands;

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

            public Builder commands(CommandsStats commands) {
                this.commands = commands;
                return this;
            }

            public Stats build() {
                return new Stats(pool, entities, events, commands);
            }
        }
    }
}
