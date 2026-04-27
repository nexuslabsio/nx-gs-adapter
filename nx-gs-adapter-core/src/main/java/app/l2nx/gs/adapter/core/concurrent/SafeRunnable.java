package app.l2nx.gs.adapter.core.concurrent;

import app.l2nx.log.NxLog;

/**
 * Wraps a {@link Runnable} so any uncaught {@link Throwable} is logged and swallowed.
 * Critical for {@link java.util.concurrent.ScheduledExecutorService} tasks: an
 * uncaught exception there cancels all subsequent invocations of the same task.
 */
public final class SafeRunnable {

    private SafeRunnable() {
    }

    public static Runnable wrap(Runnable delegate, NxLog log) {
        return () -> {
            try {
                delegate.run();
            } catch (Throwable t) {
                log.error("Wrapped runnable threw {}", t.getClass().getName());
            }
        };
    }
}
