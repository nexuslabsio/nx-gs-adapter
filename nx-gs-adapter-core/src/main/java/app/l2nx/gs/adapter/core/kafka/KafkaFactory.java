package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.kafka.KafkaState;
import java.util.Map;
import java.util.function.Consumer;

/** Test seam: bypasses the NxKafka singleton so KafkaInitializer is unit-testable without AdminClient connects or shutdown-hook side effects. */
public interface KafkaFactory {

    /**
     * Implementations MUST shut down any live NxKafka before re-init (idempotent DEGRADED to ACTIVE reconnect)
     * and MUST NOT block on broker reachability: return DISCONNECTED, nx-gs-kafka reconnects in the background.
     */
    KafkaState build(
            String brokers,
            String clientId,
            Map<String, Object> properties,
            Map<String, byte[]> staticHeaders,
            Consumer<KafkaState> stateChangeListener);
}
