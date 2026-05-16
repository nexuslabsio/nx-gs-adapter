package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.spi.HostExecutor;
import app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * {@link HostExecutor} implementation backed by a host-supplied
 * {@link Executor}. Built once per connect cycle by {@link CommandsBootstrap}
 * from the executor host code registered via
 * {@code NxAdapter.hostExecutor(Executor)}.
 *
 * <p>{@link #sync(Runnable)} / {@link #sync(Supplier)} block the calling
 * thread on a {@link CountDownLatch} the host executor counts down when the
 * task completes — bounded by {@code syncTimeoutMs} so a saturated /
 * deadlocked host pool cannot wedge the consumer thread indefinitely.
 * On timeout the call throws {@link HostExecutorTimeoutException}; the
 * dispatcher maps this to
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#UNAVAILABLE}.</p>
 *
 * <p>Exceptions thrown by the task are captured and rethrown to the calling
 * thread (using a generic-erasure {@code sneakyThrow} trick so checked
 * exceptions are NOT in scope — the SPI promises only
 * {@code RuntimeException} / {@code Error} propagation).</p>
 *
 * <p>{@link #async(Runnable)} delegates to {@code executor.execute} after
 * wrapping the task in {@link SafeRunnable} so any {@code Throwable} thrown
 * inside the task is logged by the adapter's logging facade rather than
 * leaking to the host thread's uncaught-exception handler.</p>
 *
 * <p>When no host executor has been registered, every method throws
 * {@link IllegalStateException} with a self-explanatory message — the
 * misconfiguration surfaces at the first hop instead of silently dropping
 * work.</p>
 *
 * <p>Note on interrupt semantics: if the calling thread is interrupted
 * while {@link #sync(Runnable)} / {@link #sync(Supplier)} awaits the latch,
 * the interrupt flag is restored and a {@link RuntimeException} is thrown.
 * The submitted task continues to execute on the host's pool — its result
 * is unobservable by the caller but the host thread is not leaked (the
 * latch + result holders are eligible for GC once the caller frame returns).</p>
 */
final class HostExecutorImpl implements HostExecutor {

    private static final NxLog log = NxLogFactory.getLogger(HostExecutorImpl.class);

    private static final String NOT_REGISTERED =
            "HostExecutor not registered — call NxAdapter.hostExecutor(...) before start()";

    private final @Nullable Executor delegate;
    private final long syncTimeoutMs;

    HostExecutorImpl(@Nullable Executor delegate, long syncTimeoutMs) {
        this.delegate = delegate;
        this.syncTimeoutMs = Math.max(1L, syncTimeoutMs);
    }

    @Override
    public void sync(Runnable task) {
        // sync(Supplier) does the same plumbing — adapt Runnable to a null-returning Supplier
        // so latch / error capture / interrupt translation lives in one place.
        sync(() -> {
            if (task != null) {
                task.run();
            }
            return null;
        });
    }

    @Override
    public <T> T sync(Supplier<T> task) {
        Executor exec = requireExecutor();
        if (task == null) {
            return null;
        }
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        final AtomicReference<T> result = new AtomicReference<T>();
        try {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.set(task.get());
                    } catch (Throwable t) {
                        error.set(t);
                    } finally {
                        done.countDown();
                    }
                }
            });
        } catch (Throwable submitFailure) {
            // Executor.execute rejected the task (saturated, shutting down).
            // Caller deserves to know — wrap as runtime so handler can map to
            // CommandResult.error(UNAVAILABLE, ...) explicitly if desired.
            throw rethrow(submitFailure);
        }
        boolean completed;
        try {
            completed = done.await(syncTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while awaiting host-executor task", ie);
        }
        if (!completed) {
            throw new HostExecutorTimeoutException(syncTimeoutMs);
        }
        Throwable t = error.get();
        if (t != null) {
            throw rethrow(t);
        }
        return result.get();
    }

    @Override
    public void async(Runnable task) {
        Executor exec = requireExecutor();
        if (task == null) {
            return;
        }
        // SafeRunnable.wrap routes any task-side Throwable to the adapter's logging
        // facade (NxLog) so async() failures are observable in adapter logs even when
        // the host executor's thread does not install an uncaught-exception handler.
        exec.execute(SafeRunnable.wrap(task, log));
    }

    private Executor requireExecutor() {
        Executor exec = delegate;
        if (exec == null) {
            throw new IllegalStateException(NOT_REGISTERED);
        }
        return exec;
    }

    /**
     * Sneaky-throw any {@code Throwable} as an unchecked exception. The
     * generic erasure trick lets us throw a checked exception without
     * declaring it; the {@link app.l2nx.gs.adapter.api.spi.CommandHandler}
     * contract narrows propagation to {@code RuntimeException} / {@code Error}
     * so this is fine in practice — every exception type host code throws is
     * one of those.
     */
    private static RuntimeException rethrow(Throwable t) {
        HostExecutorImpl.rethrowAs(t);
        return null; // unreachable
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void rethrowAs(Throwable t) throws E {
        throw (E) t;
    }
}
