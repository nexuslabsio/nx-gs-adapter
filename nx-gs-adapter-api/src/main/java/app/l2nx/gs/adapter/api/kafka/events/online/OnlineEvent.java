package app.l2nx.gs.adapter.api.kafka.events.online;

/**
 * Marker base for {@code events.online} family DTOs. Concrete subtypes are
 * dispatched on the platform consumer via the {@code Nx-Message-Type}
 * Kafka header (carrying the simple class name).
 */
public abstract class OnlineEvent {

    protected OnlineEvent() {
    }
}
