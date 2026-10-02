package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.kafka.KafkaState;
import java.util.Map;
import java.util.function.Consumer;

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
