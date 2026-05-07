package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.CommandHandler;
import app.l2nx.gs.adapter.api.spi.NxCommands;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

/**
 * Adapter-core implementation of {@link NxCommands}. Forwards every
 * {@code on(...)} call to {@link CommandTypeRegistry#register}; logs WARN on
 * duplicate-class re-registration to surface "two modules accidentally claim
 * the same handler" misconfigurations.
 *
 * <p>Package-private. External callers acquire an {@link NxCommands} handle
 * via {@code ConnectContext.commands()} — they never see this class directly.</p>
 */
final class NxCommandsImpl implements NxCommands {

    private static final NxLog log = NxLogFactory.getLogger(NxCommandsImpl.class);

    private final CommandTypeRegistry registry;

    NxCommandsImpl(CommandTypeRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <C extends NxCommand, R> void on(Class<C> type, CommandHandler<C, R> handler) {
        if (type == null) {
            log.warn("commands.on(null, ...) — ignoring");
            return;
        }
        if (handler == null) {
            log.warn("commands.on({}, null) — ignoring", type.getSimpleName());
            return;
        }
        boolean overwrote = registry.register(type, handler);
        if (overwrote) {
            log.warn("Re-registered handler for command type {} — previous binding replaced",
                    type.getSimpleName());
        } else {
            log.debug("Registered handler for command type {}", type.getSimpleName());
        }
    }
}
