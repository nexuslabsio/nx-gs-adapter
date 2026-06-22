package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.kafka.KafkaState;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Test stub that records the inputs handed to {@link KafkaFactory#build} so
 * unit tests can assert the property composition + listener forwarding without
 * standing up a real {@code NxKafka} singleton.
 */
public final class CapturingKafkaFactory implements KafkaFactory {

    public String capturedBrokers;
    public String capturedClientId;
    public Map<String, Object> capturedProperties;
    public Map<String, byte[]> capturedStaticHeaders;
    public Consumer<KafkaState> capturedListener;
    public int callCount;
    public KafkaState postBuildState = KafkaState.CONNECTED;

    @Override
    public KafkaState build(
            String brokers,
            String clientId,
            Map<String, Object> properties,
            Map<String, byte[]> staticHeaders,
            Consumer<KafkaState> stateChangeListener) {
        callCount++;
        capturedBrokers = brokers;
        capturedClientId = clientId;
        capturedProperties = properties;
        capturedStaticHeaders = staticHeaders;
        capturedListener = stateChangeListener;
        return postBuildState;
    }
}
