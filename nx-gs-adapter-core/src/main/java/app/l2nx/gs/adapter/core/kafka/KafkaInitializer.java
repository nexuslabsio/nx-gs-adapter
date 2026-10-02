package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.kafka.KafkaState;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/** JAAS is hard-coded to ScramLoginModule, the only SASL mechanism the platform issues. */
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

    /** Returns DISCONNECTED when the broker is unreachable within the connect timeout; the adapter reflects DEGRADED. */
    public KafkaState init(
            KafkaCredentials kafka,
            String clientId,
            Map<String, byte[]> staticHeaders,
            Consumer<KafkaState> stateChangeListener) {
        Map<String, Object> properties = new LinkedHashMap<>(producerOverrides);
        // Security properties must come after producer overrides so they win
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
