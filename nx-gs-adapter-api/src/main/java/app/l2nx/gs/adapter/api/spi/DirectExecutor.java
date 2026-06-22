package app.l2nx.gs.adapter.api.spi;

import java.util.concurrent.Executor;

/**
 * Default {@link Executor} used when {@link ConnectContext} is built without
 * an IO executor (e.g. unit tests, contexts constructed before adapter-core
 * wiring is ready). Runs the submitted {@link Runnable} synchronously on the
 * caller thread.
 *
 * <p>Package-private — host code never references this directly. The
 * direct-run fallback keeps {@code ctx.io().execute(r)} usable without an
 * adapter pool, at the cost of losing async offloading semantics. Production
 * adapter-core always injects a bounded pool.</p>
 */
final class DirectExecutor implements Executor {

    static final DirectExecutor INSTANCE = new DirectExecutor();

    private DirectExecutor() {}

    @Override
    public void execute(Runnable command) {
        command.run();
    }
}
