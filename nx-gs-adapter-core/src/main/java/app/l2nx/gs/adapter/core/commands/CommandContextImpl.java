package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.api.spi.HostExecutor;
import app.l2nx.gs.adapter.api.spi.NxEvents;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Per-invocation {@link CommandContext} implementation. Created by
 * {@link CommandsConsumer} for each polled record, passed to the handler,
 * discarded when the handler returns.
 *
 * <p>{@link #host()}, {@link #events()}, and {@link #io()} are session-scoped
 * — one instance each held by the consumer. {@link #correlationId()} is
 * per-record.</p>
 *
 * <p>Package-private. External code only sees {@link CommandContext}.</p>
 */
final class CommandContextImpl implements CommandContext {

    private final UUID correlationId;
    private final HostExecutor host;
    private final NxEvents events;
    private final Executor io;

    CommandContextImpl(UUID correlationId, HostExecutor host, NxEvents events, Executor io) {
        this.correlationId = correlationId;
        this.host = host;
        this.events = events;
        this.io = io;
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
}
