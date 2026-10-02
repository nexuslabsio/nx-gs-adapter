package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.CommandProblem;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.commons.concurrent.DaemonThreadFactory;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;

/**
 * Open deferred replies and their expiry timer. Outlives every consumer swap: a handle taken before
 * a handshake re-roll still completes afterwards, through the consumer that created it.
 */
public final class DeferredReplies {

    private static final NxLog log = NxLogFactory.getLogger(DeferredReplies.class);

    /** Bridge to the reply publisher of the consumer that dispatched the command. */
    @FunctionalInterface
    interface Publisher {
        void publish(UUID correlationId, byte[] replyMessageTypeBytes, CommandResult<?> result);
    }

    private final long maxMs;
    private final Set<DeferredReplyImpl<?>> open = ConcurrentHashMap.newKeySet();
    private final AtomicLong expiredTotal = new AtomicLong();
    private volatile @Nullable ScheduledExecutorService timer;

    public DeferredReplies(long maxMs) {
        this.maxMs = maxMs;
    }

    <R> DeferredReplyImpl<R> open(UUID correlationId, byte[] replyMessageTypeBytes, Publisher publisher) {
        DeferredReplyImpl<R> handle = new DeferredReplyImpl<R>(this, correlationId, replyMessageTypeBytes, publisher);
        open.add(handle);
        handle.armExpiry(timer().schedule(handle::expire, maxMs, TimeUnit.MILLISECONDS));
        return handle;
    }

    void closed(DeferredReplyImpl<?> handle, boolean expired) {
        open.remove(handle);
        if (expired) {
            expiredTotal.incrementAndGet();
        }
    }

    long maxMs() {
        return maxMs;
    }

    public long openCount() {
        return open.size();
    }

    public long expiredTotal() {
        return expiredTotal.get();
    }

    /**
     * Closes every open handle with {@code UNAVAILABLE} and stops the timer. Runs on adapter stop,
     * before the producer goes down, so callers get an answer instead of a silent timeout.
     */
    public void shutdown() {
        CommandResult<Object> shutdown = CommandResult.error(
                CommandStatus.UNAVAILABLE,
                CommandProblem.builder()
                        .title("Host shutting down")
                        .extension("error.cause", "host-shutdown")
                        .build());
        int closed = 0;
        for (DeferredReplyImpl<?> handle : open) {
            if (handle.completeRaw(shutdown)) {
                closed++;
            }
        }
        if (closed > 0) {
            log.info("Closed {} open deferred replies on shutdown", closed);
        }
        ScheduledExecutorService t = timer;
        if (t != null) {
            t.shutdownNow();
            timer = null;
        }
    }

    private ScheduledExecutorService timer() {
        ScheduledExecutorService t = timer;
        if (t != null) {
            return t;
        }
        synchronized (this) {
            if (timer == null) {
                timer = Executors.newSingleThreadScheduledExecutor(
                        DaemonThreadFactory.named("nx-commands-deferred", log));
            }
            return timer;
        }
    }
}
