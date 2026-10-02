package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/** Stateless façade retargeted via {@link #swap} on reconnect; null payloads are swallowed with WARN (never throws into the game loop). */
final class NxEventsImpl implements NxEvents {

    private static final NxLog log = NxLogFactory.getLogger(NxEventsImpl.class);

    private final AtomicReference<EventsPublisher> publisherRef;
    private final AtomicReference<EventTypeRegistry> registryRef;

    NxEventsImpl(EventsPublisher publisher, EventTypeRegistry registry) {
        this.publisherRef = new AtomicReference<EventsPublisher>(publisher);
        this.registryRef = new AtomicReference<EventTypeRegistry>(registry);
    }

    void swap(EventsPublisher next, EventTypeRegistry nextRegistry) {
        publisherRef.set(next);
        registryRef.set(nextRegistry);
    }

    @Override
    public void publish(@Nullable Object event) {
        if (event == null) {
            log.warn("publish called with null event — dropping");
            return;
        }
        EventsPublisher publisher = publisherRef.get();
        EventTypeRegistry registry = registryRef.get();
        if (publisher == null || registry == null) {
            log.debug("publish called before publisher wired — dropping");
            return;
        }
        EventTypeBinding binding = registry.lookup(event.getClass());
        if (binding == null) {
            log.warn(
                    "No registered binding for event type {} — dropping",
                    event.getClass().getName());
            return;
        }
        // disabled families must not burn queue capacity or inflate droppedTotal
        if (!publisher.isFamilyEnabled(binding.familyKey())) {
            log.debug("events.{} disabled — no topic configured; skipping publish", binding.familyKey());
            return;
        }
        publisher.enqueue(new EventEnvelope(event, binding));
    }

    @Override
    public boolean flush(long timeoutMs) {
        EventsPublisher publisher = publisherRef.get();
        if (publisher == null) {
            log.debug("flush called before publisher wired — nothing to flush");
            return true;
        }
        try {
            return publisher.flush(timeoutMs);
        } catch (Throwable t) {
            log.warn("flush threw {} — reporting incomplete", t.getClass().getName());
            return false;
        }
    }
}
