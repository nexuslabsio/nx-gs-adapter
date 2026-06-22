package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.commons.concurrent.DaemonThreadFactory;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Owns one {@link EntityTickLoop} per declared runtime entity. All loops share
 * a single daemon {@link ScheduledThreadPoolExecutor} sized via
 * {@code l2nx.runtime-sync.workers} — a slow snapshot for one entity briefly
 * occupies one worker but doesn't fork a thread per entity.
 */
public final class RuntimeSyncEngine {

    private static final NxLog log = NxLogFactory.getLogger(RuntimeSyncEngine.class);

    private final List<RuntimeEntityMapping<?>> mappings;
    private final TopicResolver topicResolver;
    private final SyncEventPublisher publisher;
    private final EntityStatsTracker statsTracker;
    private final EngineConfig config;

    private final List<EntityTickLoop> loops = new ArrayList<EntityTickLoop>();
    private volatile ScheduledExecutorService scheduler;

    public RuntimeSyncEngine(
            List<? extends RuntimeEntityMapping<?>> mappings,
            TopicResolver topicResolver,
            SyncEventPublisher publisher,
            EntityStatsTracker statsTracker,
            EngineConfig config) {
        this.mappings = new ArrayList<RuntimeEntityMapping<?>>(mappings);
        this.topicResolver = topicResolver;
        this.publisher = publisher;
        this.statsTracker = statsTracker;
        this.config = config;
    }

    public void start() {
        validateMappings(mappings);
        int workers = config.workers(mappings.size());
        ScheduledThreadPoolExecutor pool =
                new ScheduledThreadPoolExecutor(workers, DaemonThreadFactory.counted("nx-runtime-sync-pool-", log));
        pool.setRemoveOnCancelPolicy(true);
        this.scheduler = pool;

        for (RuntimeEntityMapping<?> mapping : mappings) {
            EntityTickLoop loop =
                    new EntityTickLoop(mapping, topicResolver, publisher, statsTracker, config, scheduler);
            loops.add(loop);
            loop.start();
            log.info(
                    "runtime-sync entity '{}' tick loop started ({}s interval)",
                    mapping.entityName(),
                    config.tickIntervalSeconds());
        }
        log.info("runtime-sync engine started: {} entities, workers={}", mappings.size(), workers);
    }

    public void stop() {
        for (EntityTickLoop loop : loops) {
            try {
                loop.stop();
            } catch (Throwable t) {
                log.warn("EntityTickLoop.stop threw {}", t.getClass().getName());
            }
        }
        ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
            try {
                // publish-flush is the largest in-flight wait per tick — give it room to drain.
                long awaitSeconds = Math.max(2L, (long) config.publishFlushSeconds() + 1L);
                if (!s.awaitTermination(awaitSeconds, TimeUnit.SECONDS)) {
                    log.warn("runtime-sync scheduler did not terminate within {}s", awaitSeconds);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        loops.clear();
    }

    private static void validateMappings(List<RuntimeEntityMapping<?>> mappings) {
        Set<String> seen = new HashSet<String>();
        for (RuntimeEntityMapping<?> m : mappings) {
            String name = m == null ? null : m.entityName();
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalStateException(
                        "RuntimeEntityMapping has null/blank entityName — refusing to start runtime-sync");
            }
            if (!seen.add(name)) {
                throw new IllegalStateException(
                        "Duplicate RuntimeEntityMapping.entityName '" + name + "' — refusing to start runtime-sync");
            }
        }
    }
}
