package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.kafka.KafkaState;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Bootstraps the {@code nx-gs-kafka} singleton from a platform-issued
 * {@link KafkaCredentials}. The state listener forwarded to the factory drives the
 * adapter's {@code ACTIVE ↔ DEGRADED} transitions once the platform handshake
 * has completed.
 *
 * <p>The JAAS line is hard-coded against
 * {@code org.apache.kafka.common.security.scram.ScramLoginModule} — the only
 * SASL mechanism the platform issues in the MVP.</p>
 */
public final class KafkaInitializer {

    private static final NxLog log = NxLogFactory.getLogger(KafkaInitializer.class);

    private static final String SCRAM_LOGIN_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule";

    private final KafkaFactory factory;
    private final Map<String, Object> producerOverrides;

    public KafkaInitializer(KafkaFactory factory) {
        this(factory, Collections.emptyMap());
    }

    public KafkaInitializer(KafkaFactory factory, Map<String, Object> producerOverrides) {
        this.factory = factory;
        this.producerOverrides = Collections.unmodifiableMap(new LinkedHashMap<>(producerOverrides));
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
     * @param staticHeaders       Kafka headers stamped on every produced record
     *                            (e.g. {@code Nx-Server-Id}); may be empty
     * @param stateChangeListener forwarded to {@code NxKafka.onStateChange}
     */
    public KafkaState init(
            KafkaCredentials kafka,
            String clientId,
            Map<String, byte[]> staticHeaders,
            Consumer<KafkaState> stateChangeListener) {
        Map<String, Object> properties = new LinkedHashMap<>(producerOverrides);
        // Security properties always win — must come after producer overrides.
        properties.put("security.protocol", kafka.getSecurityProtocol());
        properties.put("sasl.mechanism", kafka.getSaslMechanism());
        properties.put("sasl.jaas.config", buildJaas(kafka.getSaslUsername(), kafka.getSaslPassword()));

        log.info(
                "Initializing Kafka client — bootstrap={}, clientId={}, sasl.mechanism={}, staticHeaders={}",
                kafka.getBootstrap(),
                clientId,
                kafka.getSaslMechanism(),
                staticHeaders.keySet());
        return factory.build(kafka.getBootstrap(), clientId, properties, staticHeaders, stateChangeListener);
    }

    public static String buildJaas(String username, String password) {
        return SCRAM_LOGIN_MODULE + " required username=\"" + jaasEscape(username) + "\" password=\""
                + jaasEscape(password) + "\";";
    }

    private static String jaasEscape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
