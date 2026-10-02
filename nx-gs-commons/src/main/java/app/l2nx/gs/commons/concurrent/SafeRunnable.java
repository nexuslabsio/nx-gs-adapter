package app.l2nx.gs.commons.concurrent;

import app.l2nx.gs.log.NxLog;

/** Logs and swallows Throwable: an uncaught exception cancels all later runs of a scheduled task. */
public final class SafeRunnable {

    private SafeRunnable() {}

    public static Runnable wrap(Runnable delegate, NxLog log) {
        return () -> {
            try {
                delegate.run();
            } catch (Throwable t) {
                // Throwable must be the last arg for SLF4J to log the stack trace
                log.error("Wrapped runnable threw", t);
            }
        };
    }
}
