package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import java.util.List;
import java.util.function.Consumer;

/** Records published events into a sink; {@link #flush(long)} returns {@code true}. */
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
