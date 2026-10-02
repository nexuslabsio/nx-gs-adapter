package app.l2nx.gs.runtime.sync.engine.publish;

import app.l2nx.gs.adapter.api.spi.ConnectContext;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Snapshot taken at engine start; not re-resolved per tick. */
@FunctionalInterface
public interface TopicResolver {

    /** @return {@code null} when the platform published no topic; the engine then marks the entity DEGRADED. */
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

    static TopicResolver fromContext(ConnectContext ctx) {
        if (ctx == null || ctx.getSyncTopics() == null) {
            return fromSnapshot(null);
        }
        return fromSnapshot(ctx.getSyncTopics().getRuntime());
    }
}
