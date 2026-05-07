package app.l2nx.gs.adapter.api.spi;

/**
 * Thrown by {@link HostExecutor#sync(Runnable)} /
 * {@link HostExecutor#sync(java.util.function.Supplier)} when the host's
 * executor does not complete the submitted task within the configured
 * {@code l2nx.commands.host-sync-timeout-ms} window.
 *
 * <p>Operationally indicates a saturated / deadlocked host thread pool —
 * the right reply for the dispatcher is
 * {@link app.l2nx.gs.adapter.api.kafka.commands.ErrorCode#UNAVAILABLE}
 * because retrying after a delay may succeed once the pool drains.</p>
 *
 * <p>Caller MAY catch this and translate to a richer {@link
 * app.l2nx.gs.adapter.api.kafka.commands.CommandResult} (e.g. attaching
 * additional context); the adapter's commands consumer catches it
 * automatically and emits {@code UNAVAILABLE} with
 * {@code error.cause = "host-executor-timeout"}.</p>
 */
public final class HostExecutorTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long timeoutMs;

    public HostExecutorTimeoutException(long timeoutMs) {
        super("Host executor task did not complete within " + timeoutMs + "ms");
        this.timeoutMs = timeoutMs;
    }

    /**
     * Configured timeout that elapsed without the task completing.
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }
}
