package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.capability.CommandHandler;
import app.l2nx.gs.adapter.api.spi.capability.NxCommands;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The facade survives reconnect: {@link #swap(CommandTypeRegistry)} retargets the registry so modules that
 * captured {@code ctx.commands()} earlier keep working.
 */
final class NxCommandsImpl implements NxCommands {

    private static final NxLog log = NxLogFactory.getLogger(NxCommandsImpl.class);

    private final AtomicReference<CommandTypeRegistry> registryRef;

    NxCommandsImpl(CommandTypeRegistry registry) {
        this.registryRef = new AtomicReference<CommandTypeRegistry>(registry);
    }

    void swap(CommandTypeRegistry next) {
        registryRef.set(next);
    }

    CommandTypeRegistry peekRegistry() {
        return registryRef.get();
    }

    @Override
    public <R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler) {
        if (type == null) {
            log.warn("commands.on(null, ...) — ignoring");
            return;
        }
        if (handler == null) {
            log.warn("commands.on({}, null) — ignoring", type.getSimpleName());
            return;
        }
        CommandTypeRegistry registry = registryRef.get();
        if (registry == null) {
            log.warn("commands.on({}, ...) called before consumer wired — dropping", type.getSimpleName());
            return;
        }
        boolean overwrote = registry.register(type, handler);
        if (overwrote) {
            log.warn("Re-registered handler for command type {} — previous binding replaced", type.getSimpleName());
        } else {
            log.debug("Registered handler for command type {}", type.getSimpleName());
        }
    }
}
