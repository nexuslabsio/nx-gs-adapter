package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.spi.capability.HostExecutor;

/**
 * Thrown by {@link HostExecutor#sync} when the host executor misses {@code l2nx.commands.host-sync-timeout-ms}
 * (saturated or deadlocked pool). The commands consumer maps it to {@code UNAVAILABLE} with
 * {@code error.cause = "host-executor-timeout"}; retry after a delay may succeed.
 */
public final class HostExecutorTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long timeoutMs;

    public HostExecutorTimeoutException(long timeoutMs) {
        super("Host executor task did not complete within " + timeoutMs + "ms");
        this.timeoutMs = timeoutMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}
