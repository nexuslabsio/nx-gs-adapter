package app.l2nx.gs.adapter.core.modules;

import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import java.util.concurrent.atomic.AtomicReference;

public final class CapturingAdapterModule implements AdapterModule {

    private static final AtomicReference<ConnectContext> LAST_CTX = new AtomicReference<ConnectContext>();
    private static final AtomicReference<Boolean> STARTED = new AtomicReference<Boolean>(false);

    public static ConnectContext lastContext() {
        return LAST_CTX.get();
    }

    public static boolean wasStarted() {
        return Boolean.TRUE.equals(STARTED.get());
    }

    public static void reset() {
        LAST_CTX.set(null);
        STARTED.set(false);
    }

    @Override
    public String name() {
        return "capturing";
    }

    @Override
    public void onConnect(ConnectContext ctx) {
        LAST_CTX.set(ctx);
    }

    @Override
    public void start() {
        STARTED.set(true);
    }

    @Override
    public void stop() {}

    @Override
    public void onDisconnect() {}
}
