package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.spi.NxEvents;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Public factory for the events publish subsystem. {@code NxAdapter} calls
 * {@link #start(Map, EventsPublisher.Sender, EventsConfig)} once per connect
 * cycle to wire up the bounded queue + daemon thread + registry, returning a
 * {@link Started} bundle with the {@link EventsPublisher} (for shutdown +
 * heartbeat status) and the {@link NxEvents} façade (for
 * {@code ConnectContext.events()}).
 *
 * <p>Hides {@code EventTypeRegistry} and {@code NxEventsImpl} — those are
 * package-private implementation details. Callers depend only on the public
 * {@link NxEvents} interface and the {@link EventsPublisher} class.</p>
 */
public final class EventsBootstrap {

    private EventsBootstrap() {
    }

    /**
     * Materializes the registry, instantiates the publisher, starts its daemon
     * thread, and wraps the publisher in an {@link NxEvents} façade.
     *
     * @param familyTopics per-family Kafka topic map from
     *                     {@code MessagingTopics.events}; {@code null} or
     *                     empty disables every publish call (no-op +
     *                     DEBUG log).
     * @param sender       Kafka send bridge — production wires this to
     *                     {@code NxKafka.instance()::sendBytesKeyRecord}.
     * @param config       operator-tunable knobs (queue capacity, drop policy,
     *                     shutdown drain).
     */
    public static Started start(@Nullable Map<String, String> familyTopics,
                                EventsPublisher.Sender sender,
                                EventsConfig config) {
        EventTypeRegistry registry = new EventTypeRegistry();
        EventsPublisher publisher = new EventsPublisher(familyTopics, sender, config, registry);
        publisher.start();
        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        return new Started(publisher, events);
    }

    /**
     * Tuple of the wired-up publisher and its {@link NxEvents} façade.
     * {@code NxAdapter} keeps a reference to the publisher for shutdown and
     * heartbeat status; the façade goes into {@code ConnectContext.events()}.
     */
    public static final class Started {

        private final EventsPublisher publisher;
        private final NxEvents events;

        Started(EventsPublisher publisher, NxEvents events) {
            this.publisher = publisher;
            this.events = events;
        }

        public EventsPublisher publisher() {
            return publisher;
        }

        public NxEvents events() {
            return events;
        }
    }
}
