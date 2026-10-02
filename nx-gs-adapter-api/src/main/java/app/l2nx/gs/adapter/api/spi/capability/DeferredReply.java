package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.spi.CommandContext;

/**
 * Reply to a command whose outcome exists only after the handler returned — a player's answer, a
 * long host-side process. Taken with {@link CommandContext#deferReply()}; the handler then returns
 * {@link #pending()} and frees the consumer thread.
 *
 * <p>Every path that ends the work MUST call {@link #complete}. A handle left open is closed by the
 * adapter with {@code INTERNAL_ERROR} after {@code l2nx.commands.deferred-reply-max-ms}, and the
 * real outcome is lost. Open handles live in memory: a host restart drops them without a reply.</p>
 */
public interface DeferredReply<R> {

    /**
     * Hands the reply to the adapter, which publishes it under the original correlation id from its
     * own thread — never blocks the caller. Thread-safe and first-wins: {@code true} for the call
     * that won, {@code false} for every later one, including after the adapter expired the handle.
     * {@code null} or the handle's own {@link #pending()} marker is published as
     * {@code INTERNAL_ERROR}.
     */
    boolean complete(CommandResult<R> result);

    /**
     * Marker the handler returns instead of a real result. Recognised by identity and never
     * published; returning the marker of a handle taken from another context publishes
     * {@code INTERNAL_ERROR}.
     */
    CommandResult<R> pending();
}
