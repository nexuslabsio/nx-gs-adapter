package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.api.spi.ConnectContext;

/**
 * Registration of inbound command handlers, obtained via {@link ConnectContext#commands()}; implemented by
 * adapter-core. Registration is thread-safe and allowed after the consumer started; last write wins per class.
 * Routing matches {@code Nx-Message-Type} against {@code Class.getSimpleName()}, so simple names must be unique.
 * With no commands topic configured, registrations are accepted but never invoked.
 *
 * @see CommandHandler
 * @see CommandContext
 */
public interface NxCommands {

    /** Never blocks or throws; the {@code C extends NxCommand<R>} bound ties the reply payload type to the command. */
    <R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler);
}
