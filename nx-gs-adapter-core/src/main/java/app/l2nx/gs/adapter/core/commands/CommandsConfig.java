package app.l2nx.gs.adapter.core.commands;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operator-tunable knobs for the built-in commands consumer. Resolved by
 * {@code ConfigResolver.resolveCommandsConfig()} via the file-first source
 * chain ({@code l2nx.properties} → JVM system property → built-in default).
 *
 * <p>{@link #getKafkaOverrides()} are per-property overrides for the
 * {@code KafkaConsumer} this module creates (e.g.
 * {@code l2nx.commands.kafka.max.poll.records=50}). They are layered on top
 * of internal defaults — overrides win where keys collide, except for
 * security and identity properties which are always taken from the
 * platform-issued connect response.</p>
 */
public final class CommandsConfig {

    public static final long DEFAULT_POLL_TIMEOUT_MS = 100L;
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000L;
    /**
     * Bound on {@code ctx.host().sync(...)} await — high enough that a healthy
     * host pool succeeds, low enough that a stuck pool surfaces as a typed
     * {@link app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException} rather
     * than a wedged consumer thread.
     */
    public static final long DEFAULT_HOST_SYNC_TIMEOUT_MS = 30_000L;

    private final long pollTimeoutMs;
    private final long shutdownTimeoutMs;
    private final long hostSyncTimeoutMs;
    private final Map<String, Object> kafkaOverrides;

    public CommandsConfig(
            long pollTimeoutMs, long shutdownTimeoutMs, long hostSyncTimeoutMs, Map<String, Object> kafkaOverrides) {
        this.pollTimeoutMs = pollTimeoutMs;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.hostSyncTimeoutMs = hostSyncTimeoutMs;
        this.kafkaOverrides = kafkaOverrides == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Object>(kafkaOverrides));
    }

    public static CommandsConfig defaults() {
        return new CommandsConfig(
                DEFAULT_POLL_TIMEOUT_MS,
                DEFAULT_SHUTDOWN_TIMEOUT_MS,
                DEFAULT_HOST_SYNC_TIMEOUT_MS,
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

    public Map<String, Object> getKafkaOverrides() {
        return kafkaOverrides;
    }
}
