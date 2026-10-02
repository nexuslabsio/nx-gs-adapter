package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.CommandProblem;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.adapter.api.spi.capability.DeferredReply;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;

final class DeferredReplyImpl<R> implements DeferredReply<R> {

    private static final NxLog log = NxLogFactory.getLogger(DeferredReplyImpl.class);

    /**
     * The marker doubles as a valid error reply: a handler returning the marker of a handle taken
     * from another context gets it published as-is, which is exactly the documented outcome.
     */
    private final CommandResult<R> pending = CommandResult.error(
            CommandStatus.INTERNAL_ERROR,
            "Deferred marker of another command",
            "error.cause",
            "foreign-deferred-marker");

    private final DeferredReplies owner;
    private final UUID correlationId;
    private final byte[] replyMessageTypeBytes;
    private final DeferredReplies.Publisher publisher;
    private final AtomicBoolean done = new AtomicBoolean();
    private volatile @Nullable ScheduledFuture<?> expiry;

    DeferredReplyImpl(
            DeferredReplies owner,
            UUID correlationId,
            byte[] replyMessageTypeBytes,
            DeferredReplies.Publisher publisher) {
        this.owner = owner;
        this.correlationId = correlationId;
        this.replyMessageTypeBytes = replyMessageTypeBytes;
        this.publisher = publisher;
    }

    void armExpiry(ScheduledFuture<?> future) {
        expiry = future;
        if (done.get()) {
            future.cancel(false);
        }
    }

    @Override
    public boolean complete(CommandResult<R> result) {
        CommandResult<?> effective = result != null && result != pending
                ? result
                : CommandResult.error(
                        CommandStatus.INTERNAL_ERROR,
                        "Deferred reply completed with no result",
                        "error.cause",
                        "deferred-null-result");
        if (!completeRaw(effective)) {
            log.warn(
                    "Deferred reply for corr={} already completed — dropping {}", correlationId, effective.getStatus());
            return false;
        }
        return true;
    }

    @Override
    public CommandResult<R> pending() {
        return pending;
    }

    boolean isPending(@Nullable CommandResult<?> result) {
        return result == pending;
    }

    boolean completeRaw(CommandResult<?> result) {
        if (!done.compareAndSet(false, true)) {
            return false;
        }
        ScheduledFuture<?> f = expiry;
        if (f != null) {
            f.cancel(false);
        }
        owner.closed(this, false);
        owner.publish(() -> publisher.publish(correlationId, replyMessageTypeBytes, result));
        return true;
    }

    void expire() {
        if (!done.compareAndSet(false, true)) {
            return;
        }
        owner.closed(this, true);
        log.warn("Deferred reply for corr={} expired after {}ms without completion", correlationId, owner.maxMs());
        publisher.publish(
                correlationId,
                replyMessageTypeBytes,
                CommandResult.error(
                        CommandStatus.INTERNAL_ERROR,
                        CommandProblem.builder()
                                .title("Deferred reply expired")
                                .extension("error.cause", "deferred-reply-expired")
                                .extension("timeout.ms", owner.maxMs())
                                .build()));
    }
}
