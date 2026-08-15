package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;

/**
 * Single Abstract Method (SAM) for handling an inbound command. Registered
 * via {@link NxCommands#on(Class, CommandHandler)}; invoked by the adapter's
 * commands consumer thread for every record whose
 * {@code Nx-Message-Type} header matches the registered class's simple name.
 *
 * <p><b>Threading.</b> Handler runs synchronously on the consumer thread. Any
 * mutation of game-server state MUST hop to the host's executor via
 * {@link CommandContext#host()}; read-only operations and DB I/O may run on
 * the consumer thread directly.</p>
 *
 * <p><b>Return contract.</b> Non-null {@link CommandResult} required.
 * Returning {@code null} is treated as
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#INTERNAL_ERROR}
 * with detail {@code error.cause = "handler-returned-null"}.</p>
 *
 * <p><b>Exception contract.</b> Handler MAY throw {@code RuntimeException};
 * the adapter catches and replies with
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#INTERNAL_ERROR}
 * carrying the exception class + message in the reply's
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandProblem} extensions.
 * An {@code Error} (OOM, StackOverflow) is not caught: it unwinds the consumer's
 * poll loop, which is logged and stops the consumer — the {@code commands} module
 * then reports {@code DISABLED} on the heartbeat.</p>
 *
 * <p><b>Delivery is at-most-once, so handlers do NOT need to be
 * idempotent.</b> The consumer commits the batch BEFORE dispatching it, so a
 * crash or a commit failure drops the in-flight records instead of replaying
 * them: a command is delivered once or not at all, never twice. Nothing is
 * redelivered on restart, no dedup cache exists, and the caller's recovery
 * path is its own reply timeout followed by re-issuing the command.</p>
 *
 * <p><b>Type safety.</b> The bound {@code C extends NxCommand<R>} forces the
 * handler's reply payload type to match the command's declared payload type
 * at compile time — a handler for {@code DeleteAutoAnnouncementCommand} (which
 * is {@code NxCommand<Void>}) cannot return {@code CommandResult<String>}; the
 * compiler rejects it.</p>
 *
 * @param <C> concrete {@link NxCommand} subtype this handler accepts; its
 *            declared payload type {@code R} must match the handler's
 *            {@code R}.
 * @param <R> success-payload type returned via {@link CommandResult}, taken
 *            from the command's {@code NxCommand<R>} declaration. Use
 *            {@link Void} for commands declared {@code NxCommand<Void>}.
 */
@FunctionalInterface
public interface CommandHandler<C extends NxCommand<R>, R> {

    CommandResult<R> handle(C command, CommandContext ctx);
}
