package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.spi.capability.CommandHandler;
import app.l2nx.gs.adapter.api.spi.capability.DeferredReply;
import app.l2nx.gs.adapter.api.spi.capability.HostExecutor;
import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.adapter.api.spi.capability.NxSync;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Per-record context for {@link CommandHandler#handle}. Only {@link #correlationId()} is per-record;
 * the capabilities are session-scoped and stay usable after the handler returns.
 */
public interface CommandContext {

    /** Never null: a generated UUIDv7 stands in (with a WARN) when the inbound header is missing. */
    UUID correlationId();

    HostExecutor host();

    /** Same instance as {@code ConnectContext.events()}. */
    NxEvents events();

    /** Adapter-owned bounded pool ({@code l2nx.io.workers}) for blocking IO; not the game thread, use {@link #host()} for that. */
    Executor io();

    /** Same instance as {@link ConnectContext#sync()}; never null, no-op when no module registered the entity. */
    NxSync sync();

    /**
     * Takes the deferred reply: the handler returns {@link DeferredReply#pending()} and completes the handle
     * later from any thread. Repeated calls return the same handle. Take it only once the work has really
     * started; an early rejection is an ordinary immediate result.
     */
    <R> DeferredReply<R> deferReply();
}
