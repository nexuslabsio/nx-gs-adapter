package app.l2nx.gs.adapter.api.spi;

import java.util.concurrent.Executor;

/** Fallback IO executor when none is injected; runs on the caller thread, so no async offloading. */
final class DirectExecutor implements Executor {

    static final DirectExecutor INSTANCE = new DirectExecutor();

    private DirectExecutor() {}

    @Override
    public void execute(Runnable command) {
        command.run();
    }
}
