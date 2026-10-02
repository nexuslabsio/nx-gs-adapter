package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.adapter.api.spi.model.EntityMapping;
import app.l2nx.gs.adapter.api.spi.provider.JdbcConnectionSource;
import app.l2nx.gs.commons.concurrent.DaemonThreadFactory;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.db.sync.engine.persist.SnapshotPersistence;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * One shared scheduler pool ({@code l2nx.cdc-engine.workers}), one tick per entity; a per-entity
 * {@code ticking} guard prevents overlapping ticks.
 */
public final class CdcEngine {

    private static final NxLog log = NxLogFactory.getLogger(CdcEngine.class);

    private final List<EntityMapping<?>> mappings;
    private final JdbcConnectionSource jdbcSource;
    private final SnapshotStore snapshot;
    private final SnapshotPersistence persistence;
    private final EngineConfig config;
    private final TopicResolver topicResolver;
    private final SyncEventPublisher publisher;
    private final EntityStatsTracker statsTracker;
    private final WindowPlanner windowPlanner;
    private final Phase1Hasher phase1Hasher;
    private final Phase2Fetcher phase2Fetcher;
    private final String schemaName;
    private final Function<String, String> configOverrideSource;
    private final ResyncCoordinator resyncCoordinator;

    private final List<EntitySyncTask> tasks = new ArrayList<EntitySyncTask>();
    private final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Map<String, EntitySlot> slotsByEntity = new ConcurrentHashMap<String, EntitySlot>();

    private volatile ScheduledThreadPoolExecutor scheduler;

    public CdcEngine(
            String schemaName,
            List<? extends EntityMapping<?>> mappings,
            JdbcConnectionSource jdbcSource,
            SnapshotStore snapshot,
            SnapshotPersistence persistence,
            EngineConfig config,
            TopicResolver topicResolver,
            SyncEventPublisher publisher,
            EntityStatsTracker statsTracker,
            WindowPlanner windowPlanner,
            Phase1Hasher phase1Hasher,
            Phase2Fetcher phase2Fetcher,
            Function<String, String> configOverrideSource,
            NxEvents events) {
        this.schemaName = schemaName;
        this.mappings = Collections.unmodifiableList(new ArrayList<EntityMapping<?>>(mappings));
        this.jdbcSource = jdbcSource;
        this.snapshot = snapshot;
        this.persistence = persistence;
        this.config = config;
        this.topicResolver = topicResolver;
        this.publisher = publisher;
        this.statsTracker = statsTracker;
        this.windowPlanner = windowPlanner;
        this.phase1Hasher = phase1Hasher;
        this.phase2Fetcher = phase2Fetcher;
        this.configOverrideSource = configOverrideSource;
        this.resyncCoordinator = new ResyncCoordinator(events);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("CdcEngine.start called more than once — ignoring");
            return;
        }
        ConfigResolutionLogger.log(log, config, mappings, topicResolver, configOverrideSource);

        try {
            persistence.load(snapshot);
        } catch (Throwable t) {
            log.warn(
                    "SnapshotPersistence.load threw {}: {} — starting with empty snapshot",
                    t.getClass().getName(),
                    t.getMessage());
        }

        int poolSize = resolvePoolSize(config.workers(), mappings.size());
        ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(
                poolSize, DaemonThreadFactory.counted("nx-cdc-pool-" + schemaName + "-", log));
        pool.setRemoveOnCancelPolicy(true);
        this.scheduler = pool;

        for (EntityMapping<?> mapping : mappings) {
            String topic = topicResolver.resolveTopic(mapping.entityName());
            if (topic == null) {
                statsTracker.recordCycleResult(mapping.entityName(), CycleResult.degraded(0L));
            }

            EntitySyncTask task = new EntitySyncTask(
                    mapping,
                    jdbcSource,
                    snapshot,
                    windowPlanner,
                    phase1Hasher,
                    phase2Fetcher,
                    publisher,
                    topicResolver,
                    config);
            tasks.add(task);

            final String entity = mapping.entityName();
            final EntitySlot slot = new EntitySlot(task);
            slotsByEntity.put(entity, slot);
            // triggered=false never takes the targeted fast-path, so it full-scans and catches external deletes.
            Runnable tick = SafeRunnable.wrap(() -> runGuardedCycle(entity, slot, false), log);

            ScheduledFuture<?> handle = pool.scheduleWithFixedDelay(
                    tick, config.tickIntervalSeconds(), config.tickIntervalSeconds(), TimeUnit.SECONDS);
            futures.add(handle);
        }
        log.info("CdcEngine started: {} entities, schemaName={}, poolSize={}", mappings.size(), schemaName, poolSize);
    }

    /**
     * Immediate out-of-band cycle; if one is already running, a pending flag (set before submit) makes it re-submit
     * at its end, so a full cycle starts after this trigger. Non-blocking; WARN-and-drop if unknown entity or engine not running.
     */
    public void triggerEntityNow(String entityName) {
        if (!started.get() || stopped.get()) {
            log.warn("CdcEngine.triggerEntityNow({}) called before start or after stop — dropping", entityName);
            return;
        }
        EntitySlot slot = slotsByEntity.get(entityName);
        if (slot == null) {
            log.warn("CdcEngine.triggerEntityNow({}) — unknown entity, dropping", entityName);
            return;
        }
        ScheduledThreadPoolExecutor pool = scheduler;
        if (pool == null) {
            return;
        }
        // Mark before submit: if the cycle loses the CAS, the running cycle's finally re-submits.
        slot.pendingImmediate.set(true);
        try {
            pool.execute(SafeRunnable.wrap(() -> runGuardedCycle(entityName, slot, true), log));
        } catch (Throwable t) {
            log.warn(
                    "CdcEngine.triggerEntityNow({}) submit failed: {}",
                    entityName,
                    t.getClass().getName(),
                    t);
        }
    }

    /**
     * Invalidates every snapshot hash on the entity's next cycle; enqueue-only, never touches {@link SnapshotStore} on the caller thread.
     * Returns {@code false} (request dropped) when the entity is unknown or the engine is not running.
     */
    public boolean requestForceResync(UUID resyncId, String entityName) {
        if (!resyncRequestAccepted(resyncId, entityName)) {
            return false;
        }
        resyncCoordinator.enqueueAll(resyncId, entityName);
        log.info(
                "Force resync requested for WHOLE entity {} (resyncId={}) — "
                        + "full re-publication burst on the next cycle",
                entityName,
                resyncId);
        triggerEntityNow(entityName);
        return true;
    }

    /** Per-row variant; PK sets of concurrent requests union, a pending whole-entity request absorbs them. */
    public boolean requestForceResync(UUID resyncId, String entityName, LongSet pks) {
        if (!resyncRequestAccepted(resyncId, entityName)) {
            return false;
        }
        resyncCoordinator.enqueuePks(resyncId, entityName, pks);
        triggerEntityNow(entityName);
        return true;
    }

    /**
     * Per-command pk-republish carrying no {@code resyncId}, so no {@code ResyncCompletedEvent} (avoids spurious unknown-resyncId WARNs downstream).
     * Coalesced: PK sets union; no-op + WARN when entity unknown or engine not running.
     */
    public void requestPkRepublishNoEvent(String entityName, LongSet pks) {
        if (pks == null || pks.isEmpty()) {
            return;
        }
        if (!started.get() || stopped.get()) {
            log.warn(
                    "CdcEngine.requestPkRepublishNoEvent({}) called before start or after stop — dropping", entityName);
            return;
        }
        if (!slotsByEntity.containsKey(entityName)) {
            log.warn("CdcEngine.requestPkRepublishNoEvent({}) — unknown entity, dropping", entityName);
            return;
        }
        resyncCoordinator.enqueueNoEventPks(entityName, pks);
        triggerEntityNow(entityName);
    }

    private boolean resyncRequestAccepted(UUID resyncId, String entityName) {
        if (!started.get() || stopped.get()) {
            log.warn(
                    "CdcEngine.requestForceResync({}, {}) called before start or after stop — dropping",
                    entityName,
                    resyncId);
            return false;
        }
        if (!slotsByEntity.containsKey(entityName)) {
            log.warn("CdcEngine.requestForceResync({}, {}) — unknown entity, dropping", entityName, resyncId);
            return false;
        }
        return true;
    }

    private void runGuardedCycle(String entity, EntitySlot slot, boolean triggered) {
        AtomicBoolean ticking = slot.ticking;
        if (!ticking.compareAndSet(false, true)) {
            // Lost the CAS: leave pendingImmediate set so the running cycle's finally re-submits.
            log.debug("Entity {} cycle already running — out-of-band/scheduled tick skipped", entity);
            return;
        }
        try {
            // Consume before the snapshot read opens: a later trigger re-sets the flag and forces another cycle.
            slot.pendingImmediate.set(false);
            // Drain before runCycle so the planner sees invalidation sentinels in the PK envelope.
            ResyncCoordinator.DrainResult drain = resyncCoordinator.drainAndInvalidate(entity, snapshot);
            LongSet targetedPks = (triggered && drain.targetedOnly()) ? drain.targetedPks() : null;
            EntitySyncTask task = slot.task;
            long cycleStartedMs = System.currentTimeMillis();
            CycleResult result;
            try {
                result = task.runCycle(targetedPks);
            } catch (RuntimeException cycleFailure) {
                // Record a degraded result (e.g. plan-size cap) so heartbeat and the resync completion gate don't go
                // stale.
                log.warn(
                        "Entity {} cycle threw {}: {} — recording DEGRADED result",
                        entity,
                        cycleFailure.getClass().getName(),
                        cycleFailure.getMessage());
                result = CycleResult.degraded(System.currentTimeMillis() - cycleStartedMs);
                statsTracker.recordCycleResult(entity, result);
                resyncCoordinator.onCycleResult(entity, result);
                // No checkpoint: a mid-flight death may leave partially-applied snapshot state.
                return;
            }
            statsTracker.recordCycleResult(entity, result);
            resyncCoordinator.onCycleResult(entity, result);
            try {
                persistence.checkpoint(entity, snapshot);
            } catch (Throwable persistError) {
                log.warn(
                        "SnapshotPersistence.checkpoint({}) threw {}: {}",
                        entity,
                        persistError.getClass().getName(),
                        persistError.getMessage());
            }
        } finally {
            ticking.set(false);
            // Re-submit a request/trigger that hit the ticking guard mid-cycle; release ticking first so it can win the
            // CAS.
            if ((slot.pendingImmediate.get() || resyncCoordinator.hasPending(entity)) && !stopped.get()) {
                triggerEntityNow(entity);
            }
        }
    }

    private static final class EntitySlot {
        final EntitySyncTask task;
        final AtomicBoolean ticking = new AtomicBoolean(false);
        final AtomicBoolean pendingImmediate = new AtomicBoolean(false);

        EntitySlot(EntitySyncTask task) {
            this.task = task;
        }
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        for (ScheduledFuture<?> handle : futures) {
            handle.cancel(false);
        }
        futures.clear();
        boolean terminated = true;
        ScheduledThreadPoolExecutor pool = scheduler;
        if (pool != null) {
            pool.shutdownNow();
            // Cancel statements blocked in JDBC that shutdownNow can't interrupt.
            for (EntitySyncTask t : tasks) {
                try {
                    t.cancelCurrentStatement();
                } catch (Throwable ignore) {
                }
            }
            try {
                terminated = pool.awaitTermination(2L, TimeUnit.SECONDS);
                if (!terminated) {
                    log.warn("CdcEngine pool did not terminate within 2s — daemon threads will exit on JVM shutdown");
                }
            } catch (InterruptedException ie) {
                terminated = false;
                Thread.currentThread().interrupt();
            }
        }
        scheduler = null;
        tasks.clear();
        slotsByEntity.clear();
        resyncCoordinator.clear();
        // Persist only after all cycle threads stopped: a flush racing the single-writer Long2IntOpenHashMap mutation
        // is UB;
        // a stale checkpoint beats a corrupt one.
        if (terminated) {
            try {
                persistence.flushAll(snapshot);
            } catch (Throwable t) {
                log.warn(
                        "SnapshotPersistence.flushAll threw {}: {} — shutdown continues",
                        t.getClass().getName(),
                        t.getMessage());
            }
        } else {
            log.warn("CdcEngine skipping final snapshot flush — a cycle thread is still running; "
                    + "freshest state not persisted, prior checkpoint retained");
        }
        try {
            persistence.close();
        } catch (Throwable t) {
            log.warn("SnapshotPersistence.close threw {}: {}", t.getClass().getName(), t.getMessage());
        }
        snapshot.clearAll();
        log.info("CdcEngine stopped");
    }

    public List<EntityMapping<?>> mappings() {
        return mappings;
    }

    /** Test seam: runs the real scheduled path (triggered=false) for one entity and blocks. */
    Future<?> runScheduledTickNow(String entityName) {
        EntitySlot slot = slotsByEntity.get(entityName);
        if (slot == null) {
            throw new IllegalArgumentException("unknown entity " + entityName);
        }
        ScheduledThreadPoolExecutor pool = scheduler;
        if (pool == null) {
            throw new IllegalStateException("CdcEngine scheduler not available");
        }
        return pool.submit(() -> runGuardedCycle(entityName, slot, false));
    }

    /** Test seam: enqueues a no-event per-PK resync without an immediate cycle, so it survives to the next scheduled tick. */
    void enqueueNoEventPksWithoutTrigger(String entityName, LongSet pks) {
        resyncCoordinator.enqueueNoEventPks(entityName, pks);
    }

    public List<Future<?>> tickOnceSynchronously() {
        if (!started.get()) {
            throw new IllegalStateException("CdcEngine not started");
        }
        ScheduledThreadPoolExecutor pool = scheduler;
        if (pool == null) {
            throw new IllegalStateException("CdcEngine scheduler not available");
        }
        List<Future<?>> futureList = new ArrayList<Future<?>>();
        for (int i = 0; i < tasks.size(); i++) {
            final EntitySyncTask task = tasks.get(i);
            final EntityMapping<?> mapping = mappings.get(i);
            futureList.add(pool.submit(() -> {
                CycleResult result = task.runCycle();
                statsTracker.recordCycleResult(mapping.entityName(), result);
            }));
        }
        return futureList;
    }

    static int resolvePoolSize(int configuredWorkers, int entities) {
        if (configuredWorkers > 0) {
            return configuredWorkers;
        }
        int cores = Runtime.getRuntime().availableProcessors() / 2;
        if (cores < 2) {
            cores = 2;
        }
        if (entities <= 0) {
            return 2;
        }
        return Math.max(2, Math.min(entities, cores));
    }
}
