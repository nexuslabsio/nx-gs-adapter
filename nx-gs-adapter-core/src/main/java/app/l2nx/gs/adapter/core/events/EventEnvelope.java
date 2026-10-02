package app.l2nx.gs.adapter.core.events;

/** Carries the resolved binding so the daemon thread does not re-resolve the registry. */
final class EventEnvelope {

    final Object payload;
    final EventTypeBinding binding;

    EventEnvelope(Object payload, EventTypeBinding binding) {
        this.payload = payload;
        this.binding = binding;
    }
}
