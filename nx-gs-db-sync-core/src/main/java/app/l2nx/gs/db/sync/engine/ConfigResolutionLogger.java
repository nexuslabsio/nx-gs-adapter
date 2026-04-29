package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.log.NxLog;

import java.util.List;
import java.util.function.Function;

/**
 * Emits a single startup log block summarizing engine globals + per-entity
 * topic resolution. Operators read this once at adapter start to verify
 * what the engine actually picked up.
 *
 * <p>Format:</p>
 * <pre>
 * cdc-engine config: tickInterval=60s [default], rowsPerWindow=500000 [default],
 *                    queryTimeout=10s [default], publishFlush=5s [default]
 * cdc-engine entities:
 *     clan        → bohpts.gs.sync.clans
 *     character   → bohpts.gs.sync.characters
 *     item        → topic=&lt;missing — entity DEGRADED&gt;
 * </pre>
 */
public final class ConfigResolutionLogger {

    private ConfigResolutionLogger() {
    }

    public static void log(NxLog log,
                           EngineConfig effective,
                           List<? extends EntityMapping<?>> mappings,
                           TopicResolver resolver,
                           Function<String, String> overrideSource) {
        log.info("cdc-engine config: tickInterval={}s {}, rowsPerWindow={} {}, "
                        + "queryTimeout={}s {}, publishFlush={}s {}",
                effective.tickIntervalSeconds(),
                tag(overrideSource, EngineConfig.KEY_TICK_INTERVAL_SECONDS),
                effective.rowsPerWindow(),
                tag(overrideSource, EngineConfig.KEY_ROWS_PER_WINDOW),
                effective.queryTimeoutSeconds(),
                tag(overrideSource, EngineConfig.KEY_QUERY_TIMEOUT_SECONDS),
                effective.publishFlushSeconds(),
                tag(overrideSource, EngineConfig.KEY_PUBLISH_FLUSH_SECONDS));

        if (mappings == null || mappings.isEmpty()) {
            log.warn("cdc-engine entities: <none> — DbSchemaProvider returned no mappings");
            return;
        }
        log.info("cdc-engine entities:");
        for (EntityMapping<?> mapping : mappings) {
            String entity = mapping.entityName();
            String topic = resolver.resolveTopic(entity);
            if (topic == null) {
                log.warn("    {} → topic=<missing — entity DEGRADED>", entity);
            } else {
                log.info("    {} → {}", entity, topic);
            }
        }
    }

    private static String tag(Function<String, String> overrideSource, String key) {
        if (overrideSource == null) {
            return "[default]";
        }
        String raw = overrideSource.apply(key);
        if (raw == null || raw.trim().isEmpty()) {
            return "[default]";
        }
        return "[operator-override]";
    }
}
