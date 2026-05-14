package app.l2nx.gs.commons.concurrent;

import app.l2nx.gs.log.NxLog;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Daemon-thread factory with a uniform uncaught-exception handler that logs
 * via {@link NxLog} at ERROR — adapter contract requires that no daemon thread
 * dies silently in the host JVM.
 *
 * <p>Two flavors:
 * <ul>
 *     <li>{@link #named(String, NxLog)} — single fixed name, used for one-shot
 *     schedulers (e.g. {@code nx-adapter-connect}).</li>
 *     <li>{@link #counted(String, NxLog)} — {@code prefix-N} for pools where
 *     {@code N} increments per spawned thread.</li>
 * </ul>
 */
public final class DaemonThreadFactory implements ThreadFactory {

    private final String prefix;
    private final boolean numbered;
    private final NxLog log;
    private final AtomicInteger counter;

    private DaemonThreadFactory(String prefix, boolean numbered, NxLog log) {
        this.prefix = prefix;
        this.numbered = numbered;
        this.log = log;
        this.counter = numbered ? new AtomicInteger(0) : null;
    }

    public static ThreadFactory named(String name, NxLog log) {
        return new DaemonThreadFactory(name, false, log);
    }

    public static ThreadFactory counted(String prefix, NxLog log) {
        return new DaemonThreadFactory(prefix, true, log);
    }

    public static Thread newDaemonThread(String name, Runnable r, NxLog log) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler((thread, ex) ->
                log.error("Uncaught exception in {}: {}",
                        thread.getName(), ex.getClass().getName(), ex));
        return t;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = numbered ? prefix + counter.incrementAndGet() : prefix;
        return newDaemonThread(name, r, log);
    }
}
