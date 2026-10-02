package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.api.spi.capability.DeferredReply;
import app.l2nx.gs.adapter.api.spi.capability.HostExecutor;
import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.adapter.api.spi.capability.NxSync;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/**
 * Per-record; discarded when the handler returns, except a deferred reply it handed out, which lives on in
 * {@link DeferredReplies}.
 */
final class CommandContextImpl implements CommandContext {

    private final UUID correlationId;
    private final HostExecutor host;
    private final NxEvents events;
    private final Executor io;
    private final NxSync sync;
    private final DeferredReplies deferredReplies;
    private final byte[] replyMessageTypeBytes;
    private final DeferredReplies.Publisher publisher;
    private @Nullable DeferredReplyImpl<?> deferred;

    CommandContextImpl(
            UUID correlationId,
            HostExecutor host,
            NxEvents events,
            Executor io,
            NxSync sync,
            DeferredReplies deferredReplies,
            byte[] replyMessageTypeBytes,
            DeferredReplies.Publisher publisher) {
        this.correlationId = correlationId;
        this.host = host;
        this.events = events;
        this.io = io;
        this.sync = sync;
        this.deferredReplies = deferredReplies;
        this.replyMessageTypeBytes = replyMessageTypeBytes;
        this.publisher = publisher;
    }

    @Override
    public UUID correlationId() {
        return correlationId;
    }

    @Override
    public HostExecutor host() {
        return host;
    }

    @Override
    public NxEvents events() {
        return events;
    }

    @Override
    public Executor io() {
        return io;
    }

    @Override
    public NxSync sync() {
        return sync;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <R> DeferredReply<R> deferReply() {
        if (deferred == null) {
            deferred = deferredReplies.open(correlationId, replyMessageTypeBytes, publisher);
        }
        return (DeferredReply<R>) deferred;
    }

    @Nullable
    synchronized DeferredReplyImpl<?> takenDeferredReply() {
        return deferred;
    }
}
