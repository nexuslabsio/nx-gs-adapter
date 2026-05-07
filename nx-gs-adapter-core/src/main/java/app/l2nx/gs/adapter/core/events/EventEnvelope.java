package app.l2nx.gs.adapter.core.events;

/**
 * Internal queue element pairing a payload with its resolved type binding.
 * Carrying the binding alongside the payload avoids re-resolving the
 * registry on the publisher daemon thread.
 */
final class EventEnvelope {

    final Object payload;
    final EventTypeBinding binding;

    EventEnvelope(Object payload, EventTypeBinding binding) {
        this.payload = payload;
        this.binding = binding;
    }
}
