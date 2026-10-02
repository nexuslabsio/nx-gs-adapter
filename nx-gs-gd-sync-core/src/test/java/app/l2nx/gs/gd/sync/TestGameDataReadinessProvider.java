package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.spi.provider.GameDataReadinessProvider;

/** Defaults to {@code ready=true}; tests that flip it MUST restore it in {@code @AfterEach}. */
public final class TestGameDataReadinessProvider implements GameDataReadinessProvider {

    static volatile boolean ready = true;

    @Override
    public boolean ready() {
        return ready;
    }
}
