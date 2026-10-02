package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import org.jspecify.annotations.Nullable;

/** Fallback when no publisher is wired; swallows every call. */
final class NoOpEvents implements NxEvents {

    static final NoOpEvents INSTANCE = new NoOpEvents();

    private NoOpEvents() {}

    @Override
    public void publish(@Nullable Object event) {}

    @Override
    public boolean flush(long timeoutMs) {
        return true;
    }
}
