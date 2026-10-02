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
     * Publishes the reply under the original correlation id. Thread-safe and first-wins: returns
     * {@code true} for the call that published, {@code false} for every later call, including one
     * that arrives after the adapter expired the handle. A {@code null} result is published as
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
