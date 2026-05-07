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
 * {@link app.l2nx.gs.adapter.api.kafka.commands.ErrorCode#INTERNAL_ERROR}
 * with detail {@code error.cause = "handler-returned-null"}.</p>
 *
 * <p><b>Exception contract.</b> Handler MAY throw {@code RuntimeException};
 * the adapter catches and replies with
 * {@link app.l2nx.gs.adapter.api.kafka.commands.ErrorCode#INTERNAL_ERROR}
 * carrying the exception class + message in {@code errorDetails}.
 * {@code Error} (OOM, StackOverflow) propagates uncaught — the consumer
 * thread aborts and the JVM-level handler decides what to do; offset is NOT
 * committed and the record is redelivered on next start.</p>
 *
 * <p><b>Idempotency.</b> Kafka delivers at-least-once. The same
 * {@code correlationId} MAY arrive twice (mid-batch crash → redelivery).
 * Handlers MUST be idempotent; the adapter does not maintain a built-in
 * dedup cache.</p>
 *
 * @param <C> concrete {@link NxCommand} subtype this handler accepts
 * @param <R> success-payload type returned via {@link CommandResult}; use
 *            {@link Void} for void replies.
 */
@FunctionalInterface
public interface CommandHandler<C extends NxCommand, R> {

    CommandResult<R> handle(C command, CommandContext ctx);
}
