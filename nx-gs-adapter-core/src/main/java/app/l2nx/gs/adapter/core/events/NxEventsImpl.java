package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.spi.NxEvents;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

/**
 * Adapter-core implementation of {@link NxEvents}. Stateless façade — every
 * {@code publishX} method resolves the registered binding for the concrete
 * payload class and enqueues into the shared {@link EventsPublisher}.
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

    private final EventsPublisher publisher;
    private final EventTypeRegistry registry;

    NxEventsImpl(EventsPublisher publisher, EventTypeRegistry registry) {
        this.publisher = publisher;
        this.registry = registry;
    }

    @Override
    public void publishPremiumPurchase(PremiumPurchaseEvent event) {
        dispatch(event);
    }

    @Override
    public void publishServerOnlineSnapshot(ServerOnlineSnapshotEvent event) {
        dispatch(event);
    }

    @Override
    public void publishPrivateStoreSnapshot(PrivateStoreSnapshotEvent event) {
        dispatch(event);
    }

    @Override
    public void publishPrivateStorePurchase(PrivateStorePurchaseEvent event) {
        dispatch(event);
    }

    private void dispatch(Object event) {
        if (event == null) {
            log.warn("publish called with null event — dropping");
            return;
        }
        EventTypeBinding binding = registry.lookup(event.getClass());
        if (binding == null) {
            log.warn("No registered binding for event type {} — dropping", event.getClass().getName());
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
}
