package app.l2nx.gs.runtime.sync.engine.publish;

import app.l2nx.gs.adapter.api.spi.ConnectContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves entity name → Kafka topic from
 * {@code ConnectContext.syncTopics().runtime()} — the runtime namespace of the
 * platform-supplied {@link app.l2nx.gs.adapter.api.rest.SyncTopics} bundle.
 * Cached as an immutable snapshot at engine start; not re-resolved per tick.
 */
@FunctionalInterface
public interface TopicResolver {

    /**
     * @return the topic for the given entity, or {@code null} when the platform
     * did not publish a runtime topic for it. Engine treats null as "entity
     * DEGRADED, no Kafka publishes for the entity".
     */
    String resolveTopic(String entityName);

    static TopicResolver fromSnapshot(Map<String, String> source) {
        Map<String, String> snapshot;
        if (source == null || source.isEmpty()) {
            snapshot = Collections.emptyMap();
        } else {
            snapshot = Collections.unmodifiableMap(new LinkedHashMap<String, String>(source));
        }
        return entityName -> snapshot.get(entityName);
    }

    /**
     * Convenience binding to a {@link ConnectContext} — reads the {@code runtime}
     * namespace of {@code syncTopics} (runtime-sync's slice).
     */
    static TopicResolver fromContext(ConnectContext ctx) {
        if (ctx == null || ctx.getSyncTopics() == null) {
            return fromSnapshot(null);
        }
        return fromSnapshot(ctx.getSyncTopics().getRuntime());
    }
}
