package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.core.kafka.gson.AdapterGson;
import app.l2nx.gs.kafka.*;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import java.util.Map;
import java.util.function.Consumer;

/** Shuts down any live singleton before init so a reconnect that re-fetches credentials gets a fresh client. */
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

        Gson gson = AdapterGson.create();
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
            return;
        }
        if (existing.state() != KafkaState.CLOSED) {
            log.info("Existing NxKafka singleton in state {} — shutting down before re-init", existing.state());
            try {
                existing.shutdown();
            } catch (Throwable t) {
                // A faulty consumer/producer close must not bubble into the connect-scheduler thread
                log.error("NxKafka.shutdown() threw during re-init: {}", t.getMessage(), t);
            }
        }
    }
}
