package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns one {@link EntityTickLoop} per declared runtime entity. Spins up a
 * dedicated single-thread daemon scheduler per entity so a slow snapshot for
 * one entity doesn't starve the others.
 */
public final class RuntimeSyncEngine {

    private static final NxLog log = NxLogFactory.getLogger(RuntimeSyncEngine.class);

    private final List<RuntimeEntityMapping<?>> mappings;
    private final TopicResolver topicResolver;
    private final SyncEventPublisher publisher;
    private final EntityStatsTracker statsTracker;
    private final EngineConfig config;

    private final List<EntityTickLoop> loops = new ArrayList<EntityTickLoop>();
    private final List<ScheduledExecutorService> schedulers = new ArrayList<ScheduledExecutorService>();

    public RuntimeSyncEngine(List<? extends RuntimeEntityMapping<?>> mappings,
                             TopicResolver topicResolver,
                             SyncEventPublisher publisher,
                             EntityStatsTracker statsTracker,
                             EngineConfig config) {
        this.mappings = new ArrayList<>(mappings);
        this.topicResolver = topicResolver;
        this.publisher = publisher;
        this.statsTracker = statsTracker;
        this.config = config;
    }

    public void start() {
        for (RuntimeEntityMapping<?> mapping : mappings) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "nx-runtime-sync-" + mapping.entityName());
                t.setDaemon(true);
                return t;
            });
            schedulers.add(scheduler);
            EntityTickLoop loop = new EntityTickLoop(mapping, topicResolver, publisher,
                    statsTracker, config, scheduler);
            loops.add(loop);
            loop.start();
            log.info("runtime-sync entity '{}' tick loop started ({}s interval)",
                    mapping.entityName(), config.tickIntervalSeconds());
        }
    }

    public void stop() {
        for (EntityTickLoop loop : loops) {
            try {
                loop.stop();
            } catch (Throwable t) {
                log.warn("EntityTickLoop.stop threw {}", t.getClass().getName());
            }
        }
        for (ScheduledExecutorService s : schedulers) {
            s.shutdownNow();
            try {
                s.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        loops.clear();
        schedulers.clear();
    }
}
