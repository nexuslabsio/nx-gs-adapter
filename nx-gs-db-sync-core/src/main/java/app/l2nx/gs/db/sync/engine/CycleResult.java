package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;

import java.util.Objects;

/**
 * Outcome of one {@link EntitySyncTask} run, surfaced to
 * {@link EntityStatsTracker}. Counters are post-publish-walk: only PKs whose
 * Kafka publish succeeded contribute to {@code created/updated/deleted}; failed
 * publishes leave the snapshot untouched and are replayed on the next cycle.
 */
public final class CycleResult {

    private final EntityState state;
    private final long durationMs;
    private final long created;
    private final long updated;
    private final long deleted;
    private final long rowCount;

    public CycleResult(EntityState state,
                       long durationMs,
                       long created,
                       long updated,
                       long deleted,
                       long rowCount) {
        this.state = state;
        this.durationMs = durationMs;
        this.created = created;
        this.updated = updated;
        this.deleted = deleted;
        this.rowCount = rowCount;
    }

    public EntityState state() {
        return state;
    }

    public long durationMs() {
        return durationMs;
    }

    public long created() {
        return created;
    }

    public long updated() {
        return updated;
    }

    public long deleted() {
        return deleted;
    }

    public long rowCount() {
        return rowCount;
    }

    public static CycleResult degraded(long durationMs) {
        return new CycleResult(EntityState.DEGRADED, durationMs, 0L, 0L, 0L, 0L);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CycleResult)) return false;
        CycleResult that = (CycleResult) o;
        return durationMs == that.durationMs
                && created == that.created
                && updated == that.updated
                && deleted == that.deleted
                && rowCount == that.rowCount
                && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, durationMs, created, updated, deleted, rowCount);
    }

    @Override
    public String toString() {
        return "CycleResult[state=" + state
                + ", durationMs=" + durationMs
                + ", created=" + created
                + ", updated=" + updated
                + ", deleted=" + deleted
                + ", rowCount=" + rowCount + "]";
    }
}
