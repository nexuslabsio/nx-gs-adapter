package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.Objects;

/**
 * Per-entity operational snapshot populated by the CDC engine on every cycle and
 * surfaced inside {@link ModuleStatus.Stats#getEntities()}. One {@code EntityStats}
 * per synced entity (clan, character, item, …); the engine rebuilds the list per
 * cycle.
 *
 * <p>{@code name} is the entity name (e.g. {@code "clan"}, {@code "character"}) —
 * NOT the source SQL table. Counters are nullable so a partial cycle (e.g. engine
 * never finished its first tick) can still produce a status row with {@code state}
 * and {@code consecutiveErrors} set.</p>
 */
public final class EntityStats {

    private final String name;
    private final EntityState state;
    private final Long rowCount;
    private final Long lastSyncEpochMs;
    private final Long lastCycleDurationMs;
    private final ChangesSummary lastCycleChanges;
    private final Integer consecutiveErrors;

    public EntityStats(String name,
                       EntityState state,
                       Long rowCount,
                       Long lastSyncEpochMs,
                       Long lastCycleDurationMs,
                       ChangesSummary lastCycleChanges,
                       Integer consecutiveErrors) {
        this.name = name;
        this.state = state;
        this.rowCount = rowCount;
        this.lastSyncEpochMs = lastSyncEpochMs;
        this.lastCycleDurationMs = lastCycleDurationMs;
        this.lastCycleChanges = lastCycleChanges;
        this.consecutiveErrors = consecutiveErrors;
    }

    public String getName() {
        return name;
    }

    public EntityState getState() {
        return state;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public Long getLastSyncEpochMs() {
        return lastSyncEpochMs;
    }

    public Long getLastCycleDurationMs() {
        return lastCycleDurationMs;
    }

    public ChangesSummary getLastCycleChanges() {
        return lastCycleChanges;
    }

    public Integer getConsecutiveErrors() {
        return consecutiveErrors;
    }

    public Builder toBuilder() {
        return new Builder()
                .name(name)
                .state(state)
                .rowCount(rowCount)
                .lastSyncEpochMs(lastSyncEpochMs)
                .lastCycleDurationMs(lastCycleDurationMs)
                .lastCycleChanges(lastCycleChanges)
                .consecutiveErrors(consecutiveErrors);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntityStats)) return false;
        EntityStats that = (EntityStats) o;
        return Objects.equals(name, that.name)
                && state == that.state
                && Objects.equals(rowCount, that.rowCount)
                && Objects.equals(lastSyncEpochMs, that.lastSyncEpochMs)
                && Objects.equals(lastCycleDurationMs, that.lastCycleDurationMs)
                && Objects.equals(lastCycleChanges, that.lastCycleChanges)
                && Objects.equals(consecutiveErrors, that.consecutiveErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, state, rowCount, lastSyncEpochMs, lastCycleDurationMs,
                lastCycleChanges, consecutiveErrors);
    }

    @Override
    public String toString() {
        return "EntityStats[name=" + name
                + ", state=" + state
                + ", rowCount=" + rowCount
                + ", lastSyncEpochMs=" + lastSyncEpochMs
                + ", lastCycleDurationMs=" + lastCycleDurationMs
                + ", lastCycleChanges=" + lastCycleChanges
                + ", consecutiveErrors=" + consecutiveErrors + "]";
    }

    public static final class Builder {
        private String name;
        private EntityState state;
        private Long rowCount;
        private Long lastSyncEpochMs;
        private Long lastCycleDurationMs;
        private ChangesSummary lastCycleChanges;
        private Integer consecutiveErrors;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder state(EntityState state) {
            this.state = state;
            return this;
        }

        public Builder rowCount(Long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public Builder lastSyncEpochMs(Long lastSyncEpochMs) {
            this.lastSyncEpochMs = lastSyncEpochMs;
            return this;
        }

        public Builder lastCycleDurationMs(Long lastCycleDurationMs) {
            this.lastCycleDurationMs = lastCycleDurationMs;
            return this;
        }

        public Builder lastCycleChanges(ChangesSummary lastCycleChanges) {
            this.lastCycleChanges = lastCycleChanges;
            return this;
        }

        public Builder consecutiveErrors(Integer consecutiveErrors) {
            this.consecutiveErrors = consecutiveErrors;
            return this;
        }

        public EntityStats build() {
            return new EntityStats(name, state, rowCount, lastSyncEpochMs,
                    lastCycleDurationMs, lastCycleChanges, consecutiveErrors);
        }
    }
}
