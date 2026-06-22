package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import java.util.Objects;

/**
 * Outcome of one {@link EntitySyncTask} run, surfaced to
 * {@link EntityStatsTracker}. Counters are post-publish-walk: only PKs whose
 * Kafka publish succeeded contribute to {@code created/updated/deleted}; failed
 * publishes leave the snapshot untouched and are replayed on the next cycle.
 *
 * <p>{@code failedPublishes} / {@code pendingPublishes} count the publishes
 * that did NOT succeed within the cycle (failed exceptionally / still pending
 * past the flush deadline). The force-resync completion gate requires both to
 * be zero on top of a HEALTHY state — a cycle whose every publish failed
 * still reports HEALTHY, so state alone cannot prove full publication.</p>
 */
public final class CycleResult {

    private final EntityState state;
    private final long durationMs;
    private final long created;
    private final long updated;
    private final long deleted;
    private final long rowCount;
    private final long failedPublishes;
    private final long pendingPublishes;

    public CycleResult(EntityState state, long durationMs, long created, long updated, long deleted, long rowCount) {
        this(state, durationMs, created, updated, deleted, rowCount, 0L, 0L);
    }

    public CycleResult(
            EntityState state,
            long durationMs,
            long created,
            long updated,
            long deleted,
            long rowCount,
            long failedPublishes,
            long pendingPublishes) {
        this.state = state;
        this.durationMs = durationMs;
        this.created = created;
        this.updated = updated;
        this.deleted = deleted;
        this.rowCount = rowCount;
        this.failedPublishes = failedPublishes;
        this.pendingPublishes = pendingPublishes;
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

    public long failedPublishes() {
        return failedPublishes;
    }

    public long pendingPublishes() {
        return pendingPublishes;
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
                && failedPublishes == that.failedPublishes
                && pendingPublishes == that.pendingPublishes
                && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, durationMs, created, updated, deleted, rowCount, failedPublishes, pendingPublishes);
    }

    @Override
    public String toString() {
        return "CycleResult[state=" + state
                + ", durationMs=" + durationMs
                + ", created=" + created
                + ", updated=" + updated
                + ", deleted=" + deleted
                + ", rowCount=" + rowCount
                + ", failedPublishes=" + failedPublishes
                + ", pendingPublishes=" + pendingPublishes + "]";
    }
}
