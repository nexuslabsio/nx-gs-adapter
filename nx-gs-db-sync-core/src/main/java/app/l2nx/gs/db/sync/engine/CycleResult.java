package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.model.EntityState;
import java.util.Objects;

/**
 * Outcome of one {@link EntitySyncTask} run; counters count only ack'd publishes. The resync completion gate also needs
 * {@code failedPublishes} and {@code pendingPublishes} to be zero, because a cycle with every publish failed still reports HEALTHY.
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
