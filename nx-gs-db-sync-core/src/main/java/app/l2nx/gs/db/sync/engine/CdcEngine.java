package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.adapter.api.spi.NxEvents;
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
 * Top-level orchestrator. Maintains one shared scheduler pool sized by
 * {@code l2nx.cdc-engine.workers}; schedules one tick per entity at
 * {@link EngineConfig#tickIntervalSeconds()}. A per-entity {@code ticking}
 * guard prevents overlapping ticks when a previous one ran long.
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
            Runnable tick = SafeRunnable.wrap(() -> runGuardedCycle(entity, slot), log);

            ScheduledFuture<?> handle = pool.scheduleWithFixedDelay(
                    tick, config.tickIntervalSeconds(), config.tickIntervalSeconds(), TimeUnit.SECONDS);
            futures.add(handle);
        }
        log.info("CdcEngine started: {} entities, schemaName={}, poolSize={}", mappings.size(), schemaName, poolSize);
    }

    /**
     * Out-of-band trigger: submit an immediate cycle for the named entity
     * onto the engine's pool, bypassing the fixed-delay timer. Used by
     * {@code NxSync.requestNow} so command handlers can request fresh
     * sync state right after mutating an entity (e.g. after item transfer)
     * instead of waiting for the next scheduled tick.
     *
     * <p>Honors the same per-entity {@code ticking} guard as scheduled
     * runs. If a tick is already running for this entity the submitted
     * cycle CASes the guard and returns as a no-op — but the trigger is
     * NOT lost: it sets a per-entity pending-immediate flag BEFORE
     * submitting, which the in-flight cycle's {@code finally} observes and
     * re-submits, guaranteeing one full cycle starts strictly after this
     * trigger. The flag is consumed at the start of a cycle (before the
     * consistent-snapshot read opens), so a trigger landing after the read
     * re-sets it and forces yet another cycle rather than racing the read.
     * Multiple triggers during one cycle coalesce into a single re-run.
     * Unknown entity names log at WARN and drop.</p>
     *
     * <p>Engine must be {@link #start()}ed and not {@link #stop()}ped;
     * pre-start and post-stop calls log at WARN and drop. Calling thread
     * does NOT block — submission only.</p>
     *
     * @param entityName entity name as declared by
     *                   {@link EntityMapping#entityName()}
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
        // Mark BEFORE submit so a trigger racing the ticking guard is recorded:
        // if the submitted cycle loses the CAS, the running cycle's finally still
        // sees this and re-submits — the request is never silently dropped.
        slot.pendingImmediate.set(true);
        try {
            pool.execute(SafeRunnable.wrap(() -> runGuardedCycle(entityName, slot), log));
        } catch (Throwable t) {
            log.warn(
                    "CdcEngine.triggerEntityNow({}) submit failed: {}",
                    entityName,
                    t.getClass().getName(),
                    t);
        }
    }

    /**
     * Force-resync request for a whole entity: every snapshot hash is
     * invalidated on the entity's next cycle, re-publishing every live row
     * and re-emitting DELETED for snapshot-known ghosts. Thread-safe,
     * non-blocking — enqueues the request and submits an immediate cycle;
     * never mutates {@link SnapshotStore} on the calling thread.
     *
     * @return {@code false} when the entity is unknown or the engine is not
     * running (callers map this to their own error reply); the
     * request is dropped in that case
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

    /**
     * Force-resync request for selected rows of an entity. Same contract as
     * {@link #requestForceResync(UUID, String)}; PK sets of concurrent
     * requests union, a pending whole-entity request absorbs them.
     */
    public boolean requestForceResync(UUID resyncId, String entityName, LongSet pks) {
        if (!resyncRequestAccepted(resyncId, entityName)) {
            return false;
        }
        resyncCoordinator.enqueuePks(resyncId, entityName, pks);
        triggerEntityNow(entityName);
        return true;
    }

    /**
     * INTERNAL per-command pk-republish — same snapshot-perturb + immediate
     * cycle as {@link #requestForceResync(UUID, String, LongSet)} but carries
     * NO {@code resyncId}, so it emits NO {@code ResyncCompletedEvent}. Used by
     * {@code NxSync.requestResync} so command handlers can guarantee
     * re-publication of specific rows right after a mutation without registering
     * a tracked admin resync operation (which would make the platform consumer
     * log spurious unknown-resyncId WARNs per command). Thread-safe,
     * non-blocking; never mutates {@link SnapshotStore} on the calling thread —
     * the perturbation runs on the entity's cycle thread, the calling thread
     * only enqueues + submits.
     *
     * <p>Coalesced: concurrent requests union their PK sets, and a request
     * landing mid-cycle re-submits at cycle end (the same pending-request
     * machinery the tracked resync uses). No-op + WARN when the entity is
     * unknown or the engine is not running; no-op on a {@code null}/empty PK
     * set.</p>
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

    private void runGuardedCycle(String entity, EntitySlot slot) {
        AtomicBoolean ticking = slot.ticking;
        if (!ticking.compareAndSet(false, true)) {
            // Lost the CAS — a cycle is already running. Do NOT clear
            // pendingImmediate here: the running cycle's finally must observe it
            // and re-submit, otherwise an immediate trigger landing now is lost.
            log.debug("Entity {} cycle already running — out-of-band/scheduled tick skipped", entity);
            return;
        }
        try {
            // Consume pendingImmediate BEFORE the snapshot read opens: any trigger
            // arriving after this point (during or after the read) re-sets the
            // flag and forces another full cycle, so a change committed before the
            // trigger is guaranteed to be observed by some cycle. Consuming after
            // the read would lose a trigger racing the read.
            slot.pendingImmediate.set(false);
            // Drain BEFORE runCycle: the planner must see invalidation sentinels
            // in the snapshot's PK envelope, otherwise out-of-range sentinel
            // DELETEDs would slip to the next cycle.
            resyncCoordinator.drainAndInvalidate(entity, snapshot);
            EntitySyncTask task = slot.task;
            long cycleStartedMs = System.currentTimeMillis();
            CycleResult result;
            try {
                result = task.runCycle();
            } catch (RuntimeException cycleFailure) {
                // E.g. WindowPlanner's plan-size cap after a garbage PK inflated
                // the snapshot envelope. Record a degraded result so the heartbeat
                // does not keep showing the last pre-failure result forever and the
                // resync completion gate stays deferred.
                log.warn(
                        "Entity {} cycle threw {}: {} — recording DEGRADED result",
                        entity,
                        cycleFailure.getClass().getName(),
                        cycleFailure.getMessage());
                result = CycleResult.degraded(System.currentTimeMillis() - cycleStartedMs);
                statsTracker.recordCycleResult(entity, result);
                resyncCoordinator.onCycleResult(entity, result);
                // No checkpoint: the cycle died mid-flight, the snapshot may hold
                // partially-applied state not worth persisting.
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
            // A resync request OR an immediate trigger (NxSync.requestNow) that
            // landed mid-cycle hit the ticking guard as a no-op — re-submit so it
            // runs in the immediately following cycle instead of waiting for the
            // next scheduled tick. Order matters: release ticking first, then
            // observe the flags, so the re-submitted cycle can win the CAS.
            if ((slot.pendingImmediate.get() || resyncCoordinator.hasPending(entity)) && !stopped.get()) {
                triggerEntityNow(entity);
            }
        }
    }

    private static final class EntitySlot {
        final EntitySyncTask task;
        final AtomicBoolean ticking = new AtomicBoolean(false);
        // Set by triggerEntityNow before submit; consumed at cycle start, observed
        // again in the cycle's finally to re-submit a coalesced follow-up cycle so
        // an immediate trigger racing the ticking guard is never dropped.
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
            // Cancel any blocked-in-JDBC statements that shutdownNow couldn't interrupt.
            for (EntitySyncTask t : tasks) {
                try {
                    t.cancelCurrentStatement();
                } catch (Throwable ignore) {
                    // best-effort
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
        // Persist only when every cycle thread has actually stopped. A flush that
        // races a still-running cycle traverses an entity's Long2IntOpenHashMap
        // while that thread mutates it (the snapshot single-writer invariant) —
        // fastutil UB. A stale-but-intact prior checkpoint beats a snapshot
        // written under that race.
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

    /**
     * Test seam — submits one synchronous tick for each entity via the
     * shared scheduler pool.
     */
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
