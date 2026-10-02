package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.CommandProblem;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.commons.concurrent.DaemonThreadFactory;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;

/**
 * Open deferred replies, their expiry timer and the thread that publishes them. Outlives every
 * consumer swap: a handle taken before a handshake re-roll still completes afterwards, through the
 * consumer that created it.
 *
 * <p>Publishing runs on the {@code nx-commands-deferred} thread, never the caller's: a handle is
 * typically completed from a game thread, and {@code KafkaProducer.send} can block for
 * {@code max.block.ms} when metadata is missing or the buffer is full.</p>
 */
public final class DeferredReplies {

    private static final NxLog log = NxLogFactory.getLogger(DeferredReplies.class);

    private static final long SHUTDOWN_FLUSH_MS = 2_000L;

    @FunctionalInterface
    interface Publisher {
        void publish(UUID correlationId, byte[] replyMessageTypeBytes, CommandResult<?> result);
    }

    private final long maxMs;
    private final Set<DeferredReplyImpl<?>> open = ConcurrentHashMap.newKeySet();
    private final AtomicLong expiredTotal = new AtomicLong();
    private final Object lock = new Object();
    private @Nullable ScheduledThreadPoolExecutor executor;
    private boolean closed;

    public DeferredReplies(long maxMs) {
        this.maxMs = maxMs;
    }

    <R> DeferredReplyImpl<R> open(UUID correlationId, byte[] replyMessageTypeBytes, Publisher publisher) {
        DeferredReplyImpl<R> handle = new DeferredReplyImpl<R>(this, correlationId, replyMessageTypeBytes, publisher);
        synchronized (lock) {
            if (!closed) {
                handle.armExpiry(executor().schedule(handle::expire, maxMs, TimeUnit.MILLISECONDS));
                open.add(handle);
                return handle;
            }
        }
        // a handler still running after shutdown: answer at once instead of arming a dead timer
        handle.completeRaw(shutdownResult());
        return handle;
    }

    void closed(DeferredReplyImpl<?> handle, boolean expired) {
        open.remove(handle);
        if (expired) {
            expiredTotal.incrementAndGet();
        }
    }

    /** Hands a reply to the publishing thread; after shutdown it is sent inline as a last effort. */
    void publish(Runnable send) {
        Runnable safe = SafeRunnable.wrap(send, log);
        synchronized (lock) {
            if (executor != null && !executor.isShutdown()) {
                try {
                    executor.execute(safe);
                    return;
                } catch (RejectedExecutionException ignored) {
                    // fall through to the inline send
                }
            }
        }
        safe.run();
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
     * Closes every open handle with {@code UNAVAILABLE} and lets the queued replies flush. Runs on
     * adapter stop, before the producer goes down, so callers get an answer instead of a silent
     * timeout.
     */
    public void shutdown() {
        ScheduledThreadPoolExecutor toStop;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            toStop = executor;
        }
        CommandResult<Object> shutdown = shutdownResult();
        int count = 0;
        for (DeferredReplyImpl<?> handle : open) {
            if (handle.completeRaw(shutdown)) {
                count++;
            }
        }
        if (count > 0) {
            log.info("Closed {} open deferred replies on shutdown", count);
        }
        if (toStop != null) {
            toStop.shutdown();
            try {
                if (!toStop.awaitTermination(SHUTDOWN_FLUSH_MS, TimeUnit.MILLISECONDS)) {
                    toStop.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                toStop.shutdownNow();
            }
        }
    }

    private static CommandResult<Object> shutdownResult() {
        return CommandResult.error(
                CommandStatus.UNAVAILABLE,
                CommandProblem.builder()
                        .title("Host shutting down")
                        .extension("error.cause", "host-shutdown")
                        .build());
    }

    private ScheduledThreadPoolExecutor executor() {
        if (executor == null) {
            ScheduledThreadPoolExecutor created =
                    new ScheduledThreadPoolExecutor(1, DaemonThreadFactory.named("nx-commands-deferred", log));
            // a completed handle cancels its expiry; without this the cancelled task pins the handle
            // and its consumer until the original deadline
            created.setRemoveOnCancelPolicy(true);
            created.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor = created;
        }
        return executor;
    }
}
