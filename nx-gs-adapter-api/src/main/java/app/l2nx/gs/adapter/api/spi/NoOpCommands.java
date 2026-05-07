package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;

/**
 * Default {@link NxCommands} implementation used when {@link ConnectContext}
 * is built without a commands runtime (e.g. unit tests, contexts constructed
 * before the adapter-core wiring is ready). {@link #on(Class, CommandHandler)}
 * is a silent no-op — registrations are dropped on the floor.
 *
 * <p>Package-private — host code never references this directly. The contract
 * "registration before adapter is ready → silent no-op" is intentional; the
 * actual adapter-core implementation accepts registrations for late-binding.
 * This fallback exists so {@code ctx.commands().on(...)} never throws
 * {@code NullPointerException} on a partially-initialized context.</p>
 */
final class NoOpCommands implements NxCommands {

    static final NoOpCommands INSTANCE = new NoOpCommands();

    private NoOpCommands() {
    }

    @Override
    public <C extends NxCommand, R> void on(Class<C> type, CommandHandler<C, R> handler) {
        // intentional no-op — see class Javadoc
    }
}
