package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.spi.NxEvents;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * Adapter-core implementation of {@link NxEvents}. Stateless façade —
 * {@link #publish} resolves the registered binding for the concrete payload
 * class and enqueues into the shared {@link EventsPublisher}.
 *
 * <p>The façade survives reconnect: {@code NxAdapter} caches a single
 * {@code NxEventsImpl} per JVM and calls {@link #swap(EventsPublisher, EventTypeRegistry)}
 * to retarget it at a freshly built publisher. Modules that captured
 * {@code ctx.events()} from an earlier {@code onConnect} keep working
 * without re-registration.</p>
 *
 * <p>{@code null} payloads are swallowed with a WARN log (game-loop safety —
 * never throws to the caller).</p>
 *
 * <p>Package-private. External callers acquire an {@link NxEvents} handle
 * via {@link EventsBootstrap#start} or via {@code ConnectContext.events()}
 * — they never see this class directly.</p>
 */
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
        // Short-circuit: a family with no topic is "disabled" — never enqueue.
        // Otherwise disabled-family envelopes burn queue capacity and inflate
        // dropped-total against operator expectations of "no-op + DEBUG log".
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
