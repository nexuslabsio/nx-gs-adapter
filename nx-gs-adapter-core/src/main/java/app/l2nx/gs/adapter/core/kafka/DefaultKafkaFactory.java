package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import app.l2nx.gs.kafka.*;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Default {@link KafkaFactory} that bridges to {@code NxKafka.configure().build()}.
 *
 * <p>Shuts down any live singleton before init so a reconnect cycle that re-fetches
 * Kafka credentials produces a fresh client.</p>
 */
public final class DefaultKafkaFactory implements KafkaFactory {

    private static final NxLog log = NxLogFactory.getLogger(DefaultKafkaFactory.class);

    @Override
    public KafkaState build(
            String brokers,
            String clientId,
            Map<String, Object> properties,
            Map<String, byte[]> staticHeaders,
            Consumer<KafkaState> stateChangeListener) {
        shutdownExistingIfAlive();

        // Build the producer Gson here (not in nx-gs-kafka's NxGsonAdapters) so the
        // LocalizedText flat-object adapter can be registered — nx-gs-kafka must not
        // depend on nx-gs-adapter-api where LocalizedText lives.
        Gson gson = NxGsonAdapters.builder()
                .registerTypeAdapter(LocalizedText.class, new LocalizedTextTypeAdapter())
                .create();
        KafkaConfig.Builder builder = NxKafka.configure()
                .brokers(brokers)
                .clientId(clientId)
                .gson(gson)
                .onStateChange(stateChangeListener);
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            builder.property(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, byte[]> e : staticHeaders.entrySet()) {
            builder.producerStaticHeader(e.getKey(), e.getValue());
        }
        NxKafka kafka = builder.build();
        return kafka.state();
    }

    private static void shutdownExistingIfAlive() {
        NxKafka existing;
        try {
            existing = NxKafka.instance();
        } catch (KafkaException notConfigured) {
            // First init — nothing to shut down.
            return;
        }
        if (existing.state() != KafkaState.CLOSED) {
            log.info("Existing NxKafka singleton in state {} — shutting down before re-init", existing.state());
            try {
                existing.shutdown();
            } catch (Throwable t) {
                // shutdown() is internally guarded but a faulty consumer/producer close
                // still must not bubble into the connect-scheduler thread.
                log.error("NxKafka.shutdown() threw during re-init: {}", t.getMessage(), t);
            }
        }
    }
}
