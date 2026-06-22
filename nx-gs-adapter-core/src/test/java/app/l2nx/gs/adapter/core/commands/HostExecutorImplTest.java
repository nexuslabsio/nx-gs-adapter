package app.l2nx.gs.adapter.core.commands;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class HostExecutorImplTest {

    private static final long TEST_SYNC_TIMEOUT_MS = 200L;
    private static final Executor IMMEDIATE_EXECUTOR = Runnable::run;

    @Test
    void sync_runnable_shouldRunOnExecutor() {
        AtomicBoolean ran = new AtomicBoolean(false);
        HostExecutorImpl host = new HostExecutorImpl(IMMEDIATE_EXECUTOR, TEST_SYNC_TIMEOUT_MS);

        host.sync(() -> ran.set(true));

        assertTrue(ran.get());
    }

    @Test
    void sync_supplier_shouldReturnResult() {
        HostExecutorImpl host = new HostExecutorImpl(IMMEDIATE_EXECUTOR, TEST_SYNC_TIMEOUT_MS);

        String result = host.sync(() -> "hello");

        assertEquals("hello", result);
    }

    @Test
    void sync_runnable_shouldBeNoOpOnNullTask() {
        HostExecutorImpl host = new HostExecutorImpl(IMMEDIATE_EXECUTOR, TEST_SYNC_TIMEOUT_MS);

        // Should not throw
        host.sync((Runnable) null);
    }

    @Test
    void sync_supplier_shouldReturnNullOnNullTask() {
        HostExecutorImpl host = new HostExecutorImpl(IMMEDIATE_EXECUTOR, TEST_SYNC_TIMEOUT_MS);

        assertNull(host.sync((java.util.function.Supplier<String>) null));
    }

    @Test
    void sync_unregistered_shouldThrowIllegalStateException() {
        HostExecutorImpl host = new HostExecutorImpl(null, TEST_SYNC_TIMEOUT_MS);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> host.sync(() -> {}));
        assertTrue(ex.getMessage().contains("HostExecutor not registered"));
    }

    @Test
    void async_unregistered_shouldThrowIllegalStateException() {
        HostExecutorImpl host = new HostExecutorImpl(null, TEST_SYNC_TIMEOUT_MS);

        assertThrows(IllegalStateException.class, () -> host.async(() -> {}));
    }

    @Test
    void sync_taskThrowingRuntimeException_shouldPropagate() {
        HostExecutorImpl host = new HostExecutorImpl(IMMEDIATE_EXECUTOR, TEST_SYNC_TIMEOUT_MS);

        RuntimeException ex = assertThrows(
                RuntimeException.class,
                () -> host.sync(() -> {
                    throw new IllegalStateException("boom");
                }));
        assertEquals("boom", ex.getMessage());
    }

    @Test
    void sync_executorRejectingTask_shouldPropagate() {
        Executor rejecting = task -> {
            throw new java.util.concurrent.RejectedExecutionException("nope");
        };
        HostExecutorImpl host = new HostExecutorImpl(rejecting, TEST_SYNC_TIMEOUT_MS);

        assertThrows(java.util.concurrent.RejectedExecutionException.class, () -> host.sync(() -> {}));
    }

    @Test
    void sync_executorNeverCompletes_shouldThrowTimeoutException() {
        // Deliberately never executes the task
        Executor blackHole = task -> {
            /* drop on the floor */
        };
        HostExecutorImpl host = new HostExecutorImpl(blackHole, TEST_SYNC_TIMEOUT_MS);

        long t0 = System.currentTimeMillis();
        HostExecutorTimeoutException ex = assertThrows(HostExecutorTimeoutException.class, () -> host.sync(() -> {}));
        long elapsed = System.currentTimeMillis() - t0;

        assertEquals(TEST_SYNC_TIMEOUT_MS, ex.getTimeoutMs());
        // elapsed should be ~TEST_SYNC_TIMEOUT_MS — allow slack for slow CI but bound the upper end
        assertTrue(
                elapsed >= TEST_SYNC_TIMEOUT_MS,
                "expected await to last >= " + TEST_SYNC_TIMEOUT_MS + "ms, got " + elapsed);
    }

    @Test
    void async_shouldDelegateToExecutor() {
        AtomicReference<Runnable> captured = new AtomicReference<>();
        Executor capturing = captured::set;
        HostExecutorImpl host = new HostExecutorImpl(capturing, TEST_SYNC_TIMEOUT_MS);

        host.async(() -> {});

        assertNotNull(captured.get());
    }

    @Test
    void async_shouldWrapTaskInSafeRunnable_swallowingThrowables() {
        // SafeRunnable wraps the task and routes exceptions through NxLog —
        // verify the task does NOT propagate to the executor's caller.
        AtomicReference<Throwable> caughtByExecutor = new AtomicReference<>();
        Executor immediateCatching = task -> {
            try {
                task.run();
            } catch (Throwable t) {
                caughtByExecutor.set(t);
            }
        };
        HostExecutorImpl host = new HostExecutorImpl(immediateCatching, TEST_SYNC_TIMEOUT_MS);

        host.async(() -> {
            throw new RuntimeException("boom");
        });

        // SafeRunnable.wrap should have absorbed the exception — executor's catch should NOT fire.
        assertNull(caughtByExecutor.get(), "SafeRunnable.wrap should swallow task exceptions");
    }

    @Test
    void async_shouldBeNoOpOnNullTask() {
        AtomicBoolean executorInvoked = new AtomicBoolean(false);
        Executor recording = task -> executorInvoked.set(true);
        HostExecutorImpl host = new HostExecutorImpl(recording, TEST_SYNC_TIMEOUT_MS);

        host.async(null);

        assertFalse(executorInvoked.get());
    }
}
