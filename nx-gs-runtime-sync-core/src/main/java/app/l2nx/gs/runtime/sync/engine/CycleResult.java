package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.model.EntityState;
import java.util.Objects;

/** Counters are post-ack-walk: failed publishes do not count. {@code deleted} is always 0 (no tombstones) and exists only for the wire shape. */
public final class CycleResult {

    private final EntityState state;
    private final long durationMs;
    private final long created;
    private final long updated;
    private final long rowCount;
    private final long failedAcks;
    private final long timedOutAcks;

    public CycleResult(EntityState state, long durationMs, long created, long updated, long rowCount) {
        this(state, durationMs, created, updated, rowCount, 0L, 0L);
    }

    public CycleResult(
            EntityState state,
            long durationMs,
            long created,
            long updated,
            long rowCount,
            long failedAcks,
            long timedOutAcks) {
        this.state = state;
        this.durationMs = durationMs;
        this.created = created;
        this.updated = updated;
        this.rowCount = rowCount;
        this.failedAcks = failedAcks;
        this.timedOutAcks = timedOutAcks;
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

    public long rowCount() {
        return rowCount;
    }

    public long failedAcks() {
        return failedAcks;
    }

    public long timedOutAcks() {
        return timedOutAcks;
    }

    public static CycleResult degraded(long durationMs) {
        return new CycleResult(EntityState.DEGRADED, durationMs, 0L, 0L, 0L, 0L, 0L);
    }

    public static CycleResult degraded(
            long durationMs, long created, long updated, long rowCount, long failedAcks, long timedOutAcks) {
        return new CycleResult(EntityState.DEGRADED, durationMs, created, updated, rowCount, failedAcks, timedOutAcks);
    }

    public static CycleResult healthy(long durationMs, long created, long updated, long rowCount) {
        return new CycleResult(EntityState.HEALTHY, durationMs, created, updated, rowCount, 0L, 0L);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CycleResult)) return false;
        CycleResult that = (CycleResult) o;
        return durationMs == that.durationMs
                && created == that.created
                && updated == that.updated
                && rowCount == that.rowCount
                && failedAcks == that.failedAcks
                && timedOutAcks == that.timedOutAcks
                && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, durationMs, created, updated, rowCount, failedAcks, timedOutAcks);
    }

    @Override
    public String toString() {
        return "CycleResult[state=" + state
                + ", durationMs=" + durationMs
                + ", created=" + created
                + ", updated=" + updated
                + ", rowCount=" + rowCount
                + ", failedAcks=" + failedAcks
                + ", timedOutAcks=" + timedOutAcks + "]";
    }
}
