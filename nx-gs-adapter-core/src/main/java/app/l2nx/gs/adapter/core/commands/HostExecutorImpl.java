package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException;
import app.l2nx.gs.adapter.api.spi.capability.HostExecutor;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * {@code sync} waits on a latch bounded by {@code syncTimeoutMs} so a saturated host pool cannot wedge the
 * consumer thread; timeout throws {@link HostExecutorTimeoutException}. On interrupt the task keeps running
 * on the host pool but its result is dropped.
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
        // host executor threads may lack an uncaught-exception handler; wrap so failures reach the adapter log
        exec.execute(SafeRunnable.wrap(task, log));
    }

    private Executor requireExecutor() {
        Executor exec = delegate;
        if (exec == null) {
            throw new IllegalStateException(NOT_REGISTERED);
        }
        return exec;
    }

    /** Sneaky throw: the handler contract only propagates {@code RuntimeException} / {@code Error}. */
    private static RuntimeException rethrow(Throwable t) {
        HostExecutorImpl.rethrowAs(t);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void rethrowAs(Throwable t) throws E {
        throw (E) t;
    }
}
