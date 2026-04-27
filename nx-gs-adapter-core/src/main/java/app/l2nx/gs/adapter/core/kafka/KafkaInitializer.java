package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.rest.KafkaConfig;
import app.l2nx.gs.kafka.KafkaState;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Bootstraps the {@code nx-gs-kafka} singleton from a platform-issued
 * {@link KafkaConfig}. The state listener forwarded to the factory drives the
 * adapter's {@code ACTIVE ↔ DEGRADED} transitions once the platform handshake
 * has completed.
 *
 * <p>The JAAS line is hard-coded against
 * {@code org.apache.kafka.common.security.scram.ScramLoginModule} — the only
 * SASL mechanism the platform issues in the MVP.</p>
 */
public final class KafkaInitializer {

    private static final NxLog log = NxLogFactory.getLogger(KafkaInitializer.class);

    private static final String SCRAM_LOGIN_MODULE =
            "org.apache.kafka.common.security.scram.ScramLoginModule";

    private final KafkaFactory factory;

    public KafkaInitializer(KafkaFactory factory) {
        this.factory = factory;
    }

    /**
     * Build the Kafka client and return the post-build state. Returns
     * {@link KafkaState#DISCONNECTED} when the broker is unreachable
     * inside the connect timeout — the adapter should reflect this as
     * {@code DEGRADED}; {@code nx-gs-kafka} reconnects in the background.
     *
     * @param kafka               wire payload from the platform handshake
     * @param clientId            composed client identifier
     *                            ({@code nx-gs-adapter-<tenant>-<server>})
     * @param stateChangeListener forwarded to {@code NxKafka.onStateChange}
     */
    public KafkaState init(KafkaConfig kafka,
                           String clientId,
                           Consumer<KafkaState> stateChangeListener) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("security.protocol", kafka.getSecurityProtocol());
        properties.put("sasl.mechanism", kafka.getSaslMechanism());
        properties.put("sasl.jaas.config", buildJaas(kafka.getSaslUsername(), kafka.getSaslPassword()));

        log.info("Initializing Kafka client — bootstrap={}, clientId={}, sasl.mechanism={}",
                kafka.getBootstrap(), clientId, kafka.getSaslMechanism());
        return factory.build(kafka.getBootstrap(), clientId, properties, stateChangeListener);
    }

    static String buildJaas(String username, String password) {
        return SCRAM_LOGIN_MODULE + " required username=\"" + username + "\" password=\"" + password + "\";";
    }
}
