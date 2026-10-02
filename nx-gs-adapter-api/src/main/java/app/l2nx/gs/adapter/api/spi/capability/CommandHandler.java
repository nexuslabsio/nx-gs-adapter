package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.CommandContext;

/**
 * Runs synchronously on the commands consumer thread; mutate game state only via {@link CommandContext#host()}.
 * Returning null yields {@code INTERNAL_ERROR} ({@code handler-returned-null}); a thrown {@code RuntimeException}
 * yields {@code INTERNAL_ERROR} with class and message, while an {@code Error} stops the consumer
 * (heartbeat {@code commands} slot goes {@code DISABLED}).
 *
 * <p>Delivery is at-most-once (batch committed before dispatch): handlers need not be idempotent, and the
 * caller recovers by re-issuing after its own reply timeout.</p>
 *
 * @param <C> command type; its declared payload type must match {@code R}
 * @param <R> success payload type; {@link Void} for {@code NxCommand<Void>}
 */
@FunctionalInterface
public interface CommandHandler<C extends NxCommand<R>, R> {

    CommandResult<R> handle(C command, CommandContext ctx);
}
