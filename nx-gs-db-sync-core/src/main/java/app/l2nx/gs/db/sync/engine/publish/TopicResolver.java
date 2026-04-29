package app.l2nx.gs.db.sync.engine.publish;

import app.l2nx.gs.adapter.api.spi.ConnectContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves entity name → Kafka topic from the platform-supplied
 * {@code ConnectContext.syncTopics()} map. Cached as an immutable snapshot at
 * engine start; not re-resolved per cycle (a topic re-key would arrive only on
 * a fresh handshake, which already triggers a full engine rebuild).
 */
@FunctionalInterface
public interface TopicResolver {

    /**
     * @return the topic name for the given entity, or {@code null} if the
     * platform did not publish a topic for it. Engine treats null as
     * "entity DEGRADED, no Kafka publishes for the entity".
     */
    String resolveTopic(String entityName);

    /**
     * Snapshot factory: copies the supplied map at engine start so a later
     * mutation of the source map (defensive code only — {@code ConnectContext}
     * already exposes an unmodifiable view) cannot affect the running engine.
     */
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
     * Convenience binding to a {@link ConnectContext}.
     */
    static TopicResolver fromContext(ConnectContext ctx) {
        return fromSnapshot(ctx == null ? null : ctx.getSyncTopics());
    }
}
