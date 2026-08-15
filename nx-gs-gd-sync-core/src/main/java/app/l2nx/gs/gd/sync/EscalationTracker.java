package app.l2nx.gs.gd.sync;

import java.util.function.LongSupplier;

/**
 * Severity decision for a condition that is expected at first and alarming only once it persists —
 * "the host has not loaded its game data yet" being the case this exists for. Keeps the decision out
 * of the logging call sites so it can be tested without scraping log output.
 *
 * <p>Synchronized rather than thread-confined: consecutive snapshot passes run on different threads
 * of the adapter IO pool, and the readiness instance is touched by the scheduler thread. Contention
 * is nil — at most one observation per entity per pass.</p>
 */
final class EscalationTracker {

    enum Stage {
        /** First observation since the last {@link #reset()} — worth saying once. */
        FIRST,
        /** Still going, still inside the grace window. */
        REPEAT,
        /** Grace window expired on this observation — worth one alarm. */
        ESCALATED,
        /** Already escalated; the alarm was raised and must not repeat. */
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

    /** Clears the history so a condition that recovers and returns later is reported afresh. */
    synchronized void reset() {
        observed = false;
        escalated = false;
        firstObservedAt = 0L;
        lastStage = null;
    }

    /** Package-visible for tests: the stage the last {@link #observe()} returned, {@code null} if none. */
    synchronized Stage lastStage() {
        return lastStage;
    }
}
