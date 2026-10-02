package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/** Public entry point; {@code EventTypeRegistry} and {@code NxEventsImpl} stay package-private. */
public final class EventsBootstrap {

    private EventsBootstrap() {}

    /** Null or empty {@code familyTopics} disables every publish call (no-op + DEBUG log). */
    public static Started start(
            @Nullable Map<String, String> familyTopics,
            EventsPublisher.Sender sender,
            EventsPublisher.ProducerFlusher producerFlusher,
            EventsConfig config) {
        EventTypeRegistry registry = new EventTypeRegistry();
        EventsPublisher publisher = new EventsPublisher(familyTopics, sender, producerFlusher, config, registry);
        publisher.start();
        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        return new Started(publisher, events);
    }

    /** Rebuilds the publisher behind a stable façade so modules holding {@code ctx.events()} survive reconnect. */
    public static EventsPublisher swap(
            NxEvents facade,
            @Nullable Map<String, String> familyTopics,
            EventsPublisher.Sender sender,
            EventsPublisher.ProducerFlusher producerFlusher,
            EventsConfig config) {
        if (!(facade instanceof NxEventsImpl)) {
            throw new IllegalArgumentException("swap() requires a facade produced by EventsBootstrap.start(); got "
                    + (facade == null ? "null" : facade.getClass().getName()));
        }
        EventTypeRegistry registry = new EventTypeRegistry();
        EventsPublisher publisher = new EventsPublisher(familyTopics, sender, producerFlusher, config, registry);
        publisher.start();
        ((NxEventsImpl) facade).swap(publisher, registry);
        return publisher;
    }

    /** Publisher is kept for shutdown and heartbeat status; the façade goes into {@code ConnectContext.events()}. */
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
