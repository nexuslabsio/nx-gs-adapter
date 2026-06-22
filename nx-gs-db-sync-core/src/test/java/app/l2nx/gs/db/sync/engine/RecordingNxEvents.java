package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.spi.NxEvents;
import java.util.List;
import java.util.function.Consumer;

/**
 * Test double for {@link NxEvents} — records published events via a supplied
 * {@link Consumer} sink (e.g. a list's {@code add}). {@link #flush(long)} is a
 * no-op returning {@code true}. Replaces the former lambda usage now that
 * {@code NxEvents} carries a second method and is no longer a SAM.
 */
final class RecordingNxEvents implements NxEvents {

    private final Consumer<Object> sink;

    RecordingNxEvents(Consumer<Object> sink) {
        this.sink = sink;
    }

    static RecordingNxEvents into(List<Object> sink) {
        return new RecordingNxEvents(sink::add);
    }

    static RecordingNxEvents noop() {
        return new RecordingNxEvents(event -> {});
    }

    @Override
    public void publish(Object event) {
        sink.accept(event);
    }

    @Override
    public boolean flush(long timeoutMs) {
        return true;
    }
}
