package app.l2nx.gs.adapter.core.commands;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link #getKafkaOverrides()} layer over internal consumer defaults, but security and identity properties
 * always come from the platform-issued connect response.
 */
public final class CommandsConfig {

    public static final long DEFAULT_POLL_TIMEOUT_MS = 100L;
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000L;
    public static final long DEFAULT_HOST_SYNC_TIMEOUT_MS = 30_000L;

    public static final long DEFAULT_DEFERRED_REPLY_MAX_MS = 300_000L;

    private final long pollTimeoutMs;
    private final long shutdownTimeoutMs;
    private final long hostSyncTimeoutMs;
    private final long deferredReplyMaxMs;
    private final Map<String, Object> kafkaOverrides;

    public CommandsConfig(
            long pollTimeoutMs,
            long shutdownTimeoutMs,
            long hostSyncTimeoutMs,
            long deferredReplyMaxMs,
            Map<String, Object> kafkaOverrides) {
        this.pollTimeoutMs = pollTimeoutMs;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.hostSyncTimeoutMs = hostSyncTimeoutMs;
        this.deferredReplyMaxMs = deferredReplyMaxMs;
        this.kafkaOverrides = kafkaOverrides == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Object>(kafkaOverrides));
    }

    public static CommandsConfig defaults() {
        return new CommandsConfig(
                DEFAULT_POLL_TIMEOUT_MS,
                DEFAULT_SHUTDOWN_TIMEOUT_MS,
                DEFAULT_HOST_SYNC_TIMEOUT_MS,
                DEFAULT_DEFERRED_REPLY_MAX_MS,
                Collections.<String, Object>emptyMap());
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public long getShutdownTimeoutMs() {
        return shutdownTimeoutMs;
    }

    public long getHostSyncTimeoutMs() {
        return hostSyncTimeoutMs;
    }

    public long getDeferredReplyMaxMs() {
        return deferredReplyMaxMs;
    }

    public Map<String, Object> getKafkaOverrides() {
        return kafkaOverrides;
    }
}
