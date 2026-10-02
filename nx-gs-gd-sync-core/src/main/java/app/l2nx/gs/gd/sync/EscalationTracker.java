package app.l2nx.gs.gd.sync;

import java.util.function.LongSupplier;

/**
 * WARN-then-ERROR decision for a condition that is expected at first and alarming only once it persists.
 * Synchronized: passes run on different IO threads and the scheduler thread.
 */
final class EscalationTracker {

    enum Stage {
        FIRST,
        REPEAT,
        ESCALATED,
        SILENT
    }

    private final long graceMs;
    private final LongSupplier clock;

    private long firstObservedAt;
    private boolean observed;
    private boolean escalated;
    private Stage lastStage;

    EscalationTracker(long graceMs, LongSupplier clock) {
        this.graceMs = graceMs;
        this.clock = clock;
    }

    synchronized Stage observe() {
        lastStage = decide();
        return lastStage;
    }

    private Stage decide() {
        if (escalated) {
            return Stage.SILENT;
        }
        long now = clock.getAsLong();
        if (!observed) {
            observed = true;
            firstObservedAt = now;
            return Stage.FIRST;
        }
        if (now - firstObservedAt >= graceMs) {
            escalated = true;
            return Stage.ESCALATED;
        }
        return Stage.REPEAT;
    }

    synchronized void reset() {
        observed = false;
        escalated = false;
        firstObservedAt = 0L;
        lastStage = null;
    }

    synchronized Stage lastStage() {
        return lastStage;
    }
}
