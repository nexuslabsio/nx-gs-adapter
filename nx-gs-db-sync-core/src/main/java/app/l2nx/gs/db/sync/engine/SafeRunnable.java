package app.l2nx.gs.db.sync.engine;

import app.l2nx.log.NxLog;

/**
 * Wraps a {@link Runnable} so any uncaught {@link Throwable} is logged and
 * swallowed. Critical for {@link java.util.concurrent.ScheduledExecutorService}
 * tasks: an uncaught exception there cancels all subsequent invocations.
 *
 * <p>Mirrors the same-named utility in {@code :nx-gs-adapter-core}, duplicated
 * here because db-sync-core cannot depend on adapter-core (api-only contract).</p>
 */
final class SafeRunnable {

    private SafeRunnable() {
    }

    static Runnable wrap(Runnable delegate, NxLog log) {
        return () -> {
            try {
                delegate.run();
            } catch (Throwable t) {
                log.error("Wrapped runnable threw {}", t.getClass().getName());
            }
        };
    }
}
