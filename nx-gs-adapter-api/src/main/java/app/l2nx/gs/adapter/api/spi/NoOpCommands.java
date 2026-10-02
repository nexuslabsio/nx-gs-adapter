package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.capability.CommandHandler;
import app.l2nx.gs.adapter.api.spi.capability.NxCommands;

/** Fallback when no commands runtime is wired; registrations are dropped so {@code ctx.commands()} is never null-unsafe. */
final class NoOpCommands implements NxCommands {

    static final NoOpCommands INSTANCE = new NoOpCommands();

    private NoOpCommands() {}

    @Override
    public <R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler) {}
}
