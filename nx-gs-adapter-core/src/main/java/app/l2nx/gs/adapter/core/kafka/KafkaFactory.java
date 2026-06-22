package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.kafka.KafkaState;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Test seam over the {@code NxKafka.configure().build()} singleton bootstrap.
 *
 * <p>The default impl ({@link DefaultKafkaFactory}) wires the actual call chain.
 * Tests substitute a captor that records the inputs without standing up a real
 * Kafka client — bypassing the singleton makes {@link KafkaInitializer} unit-testable
 * and decouples adapter wiring from {@code nx-gs-kafka}'s build-time side effects
 * (AdminClient connect attempt + JVM shutdown hook registration).</p>
 */
public interface KafkaFactory {

    /**
     * Create or replace the {@code NxKafka} singleton with this configuration.
     *
     * <p>Implementations MUST shut down any existing live {@code NxKafka} instance
     * before re-init, so a {@code DEGRADED → ACTIVE} reconnect cycle that re-fetches
     * Kafka credentials remains idempotent.</p>
     *
     * <p>Implementations MUST NOT block on broker reachability — return immediately
     * with {@link KafkaState#DISCONNECTED} if the cluster is unreachable.
     * {@code nx-gs-kafka} keeps a background reconnect loop that will recover.</p>
     *
     * @param brokers             comma-separated bootstrap servers
     * @param clientId            client identifier — typically
     *                            {@code nx-gs-adapter-<tenant-slug>-<server-slug>}
     * @param properties          raw Kafka client properties (security.protocol,
     *                            sasl.mechanism, sasl.jaas.config)
     * @param staticHeaders       Kafka headers stamped on every produced record
     *                            (e.g. {@code Nx-Server-Id} resolved from the
     *                            connect response); may be empty
     * @param stateChangeListener invoked on every {@link KafkaState} transition
     *                            after build (CONNECTED ↔ DISCONNECTED, → CLOSED)
     * @return the post-build state — {@link KafkaState#CONNECTED} or
     * {@link KafkaState#DISCONNECTED}
     */
    KafkaState build(
            String brokers,
            String clientId,
            Map<String, Object> properties,
            Map<String, byte[]> staticHeaders,
            Consumer<KafkaState> stateChangeListener);
}
