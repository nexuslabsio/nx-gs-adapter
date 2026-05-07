package app.l2nx.gs.adapter.api.spi;

import java.util.function.Supplier;

/**
 * Host-supplied {@link java.util.concurrent.Executor} wrapper exposed to
 * command handlers via {@link CommandContext#host()}. Provides ergonomic
 * sync / async helpers around the host's game-side thread pool — typically
 * the L2 fork's {@code ThreadPoolManager.executeGeneral(...)} or equivalent.
 *
 * <p>The "host thread" terminology is a metaphor — most L2 forks have a
 * thread <em>pool</em> rather than a single game-loop thread. Hopping via
 * this SPI ensures handler-side work runs on the pool the host expects for
 * game-state mutations, preserving the host's lock-ordering and threading
 * conventions.</p>
 *
 * <p>Registered once at adapter bootstrap via
 * {@code NxAdapter.hostExecutor(Executor)} BEFORE {@code NxAdapter.start()}.
 * If unregistered while {@code commandsTopic} is configured, every
 * {@link #sync(Runnable)} / {@link #sync(Supplier)} / {@link #async(Runnable)}
 * call throws {@link IllegalStateException} — the misconfiguration surfaces
 * at the first hop instead of silently dropping work.</p>
 *
 * <p><b>Timeout contract.</b> {@link #sync(Runnable)} and
 * {@link #sync(Supplier)} await the task with a bounded timeout (configurable
 * via {@code l2nx.commands.host-sync-timeout-ms}, default 30000ms). If the
 * host's executor does not complete the task within the window, the call
 * throws {@link HostExecutorTimeoutException}. The adapter's commands
 * consumer catches this and emits an
 * {@link app.l2nx.gs.adapter.api.kafka.commands.ErrorCode#UNAVAILABLE} reply.
 * This bound is load-bearing: an unbounded await would let a saturated host
 * pool wedge the consumer thread indefinitely.</p>
 *
 * <p>Read-only handlers do not need to hop and may avoid this SPI entirely.
 * State-mutating handlers MUST hop.</p>
 */
public interface HostExecutor {

    /**
     * Run {@code task} on the host's executor and block the caller until it
     * completes or the timeout elapses. {@code RuntimeException} thrown by
     * the task propagates to the caller; {@code Error} (OOM, StackOverflow)
     * propagates as well.
     *
     * @param task non-null Runnable
     * @throws IllegalStateException        if no host executor is registered
     * @throws HostExecutorTimeoutException if the host executor does not
     *                                      complete the task within the
     *                                      configured timeout window
     */
    void sync(Runnable task);

    /**
     * Run {@code task} on the host's executor, block the caller until it
     * completes or the timeout elapses, return the result. Exception
     * semantics same as {@link #sync(Runnable)}.
     *
     * @param task non-null Supplier
     * @param <T>  return type
     * @return value returned by the task
     * @throws IllegalStateException        if no host executor is registered
     * @throws HostExecutorTimeoutException if the host executor does not
     *                                      complete the task within the
     *                                      configured timeout window
     */
    <T> T sync(Supplier<T> task);

    /**
     * Schedule {@code task} on the host's executor; do NOT wait for completion.
     * Fire-and-forget. The adapter wraps the task so that any
     * {@code Throwable} is logged via the adapter's logging facade — the
     * caller does not observe it.
     *
     * @param task non-null Runnable
     * @throws IllegalStateException if no host executor is registered
     */
    void async(Runnable task);
}
