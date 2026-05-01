package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Top-level orchestrator: spawns one daemon scheduler thread per entity, fires
 * the first tick immediately (initial sync), schedules subsequent ticks at
 * {@link EngineConfig#tickIntervalSeconds()}. Per-tick work is delegated to
 * {@link EntitySyncTask}; results land in {@link EntityStatsTracker}.
 *
 * <p>Lifecycle is owned by {@code DbSyncModule}: {@link #start} once on
 * connect, {@link #stop} once on disconnect. Idempotent.</p>
 */
public final class CdcEngine {

    private static final NxLog log = NxLogFactory.getLogger(CdcEngine.class);

    private final List<EntityMapping<?>> mappings;
    private final JdbcConnectionSource jdbcSource;
    private final SnapshotStore snapshot;
    private final EngineConfig config;
    private final TopicResolver topicResolver;
    private final SyncEventPublisher publisher;
    private final EntityStatsTracker statsTracker;
    private final WindowPlanner windowPlanner;
    private final Phase1Hasher phase1Hasher;
    private final Phase2Fetcher phase2Fetcher;
    private final String schemaName;
    private final Function<String, String> configOverrideSource;

    private final List<ScheduledExecutorService> schedulers = new ArrayList<ScheduledExecutorService>();
    private final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public CdcEngine(String schemaName,
                     List<? extends EntityMapping<?>> mappings,
                     JdbcConnectionSource jdbcSource,
                     SnapshotStore snapshot,
                     EngineConfig config,
                     TopicResolver topicResolver,
                     SyncEventPublisher publisher,
                     EntityStatsTracker statsTracker,
                     WindowPlanner windowPlanner,
                     Phase1Hasher phase1Hasher,
                     Phase2Fetcher phase2Fetcher,
                     Function<String, String> configOverrideSource) {
        this.schemaName = schemaName;
        this.mappings = Collections.unmodifiableList(new ArrayList<EntityMapping<?>>(mappings));
        this.jdbcSource = jdbcSource;
        this.snapshot = snapshot;
        this.config = config;
        this.topicResolver = topicResolver;
        this.publisher = publisher;
        this.statsTracker = statsTracker;
        this.windowPlanner = windowPlanner;
        this.phase1Hasher = phase1Hasher;
        this.phase2Fetcher = phase2Fetcher;
        this.configOverrideSource = configOverrideSource;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("CdcEngine.start called more than once — ignoring");
            return;
        }
        ConfigResolutionLogger.log(log, config, mappings, topicResolver, configOverrideSource);

        for (EntityMapping<?> mapping : mappings) {
            String topic = topicResolver.resolveTopic(mapping.entityName());
            if (topic == null) {
                statsTracker.recordCycleResult(mapping.entityName(), CycleResult.degraded(0L));
            }
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                    threadFactory(schemaName, mapping.entityName()));
            schedulers.add(scheduler);

            EntitySyncTask task = new EntitySyncTask(
                    mapping, jdbcSource, snapshot,
                    windowPlanner, phase1Hasher, phase2Fetcher,
                    publisher, topicResolver, config);

            Runnable tick = SafeRunnable.wrap(() -> {
                CycleResult result = task.runCycle();
                statsTracker.recordCycleResult(mapping.entityName(), result);
            }, log);

            ScheduledFuture<?> handle = scheduler.scheduleWithFixedDelay(
                    tick,
                    0L,
                    config.tickIntervalSeconds(),
                    TimeUnit.SECONDS);
            futures.add(handle);
        }
        log.info("CdcEngine started: {} entities, schemaName={}", mappings.size(), schemaName);
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        for (ScheduledFuture<?> handle : futures) {
            handle.cancel(false);
        }
        futures.clear();
        for (ScheduledExecutorService scheduler : schedulers) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(2L, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        schedulers.clear();
        snapshot.clearAll();
        log.info("CdcEngine stopped");
    }

    public List<EntityMapping<?>> mappings() {
        return mappings;
    }

    /**
     * Test seam — submits one synchronous tick for each entity. Used by
     * tests that prefer not to deal with the scheduler timing.
     */
    public List<Future<?>> tickOnceSynchronously() {
        if (!started.get()) {
            throw new IllegalStateException("CdcEngine not started");
        }
        List<Future<?>> futureList = new ArrayList<Future<?>>();
        for (int i = 0; i < schedulers.size(); i++) {
            ScheduledExecutorService scheduler = schedulers.get(i);
            EntityMapping<?> mapping = mappings.get(i);
            EntitySyncTask task = new EntitySyncTask(
                    mapping, jdbcSource, snapshot,
                    windowPlanner, phase1Hasher, phase2Fetcher,
                    publisher, topicResolver, config);
            futureList.add(scheduler.submit(() -> {
                CycleResult result = task.runCycle();
                statsTracker.recordCycleResult(mapping.entityName(), result);
            }));
        }
        return futureList;
    }

    private static ThreadFactory threadFactory(String schema, String entity) {
        return r -> {
            Thread t = new Thread(r, "nx-cdc-" + schema + "-" + entity);
            t.setDaemon(true);
            return t;
        };
    }
}
