package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeRow;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * One tick loop per declared runtime entity. Dispatched onto a shared scheduler
 * pool. Each tick:
 *
 * <ol>
 *     <li>Call {@code mapping.snapshot()} → {@code Iterable<RuntimeRow<T>>}.</li>
 *     <li>Hash each row via {@code mapping.hash(dto)} into a fresh
 *     {@link Long2LongOpenHashMap pk → hash}.</li>
 *     <li>Diff against the previous tick:
 *         <ul>
 *             <li>NEW (pk in current, not in prev) → publish {@code CREATED}.</li>
 *             <li>CHANGED (pk in both, hash differs) → publish {@code UPDATED}.</li>
 *             <li>GONE (pk in prev, not in current) → silently drop. No tombstone.</li>
 *         </ul>
 *     </li>
 *     <li>Wait up to {@code publishFlushSeconds} for Kafka acks; advance
 *     {@code prev} only for PKs whose ack arrived (failed publishes replay
 *     next tick).</li>
 * </ol>
 */
public final class EntityTickLoop {

    /**
     * Sentinel — fastutil's default-return-value collides with a legit hash of 0.
     */
    static final long MISSING_HASH = Long.MIN_VALUE;

    private static final NxLog log = NxLogFactory.getLogger(EntityTickLoop.class);

    private final RuntimeEntityMapping<Object> mapping;
    private final String entityName;
    private final TopicResolver topicResolver;
    private final SyncEventPublisher publisher;
    private final EntityStatsTracker statsTracker;
    private final EngineConfig config;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean ticking = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Long2LongMap prevSnapshot = newHashMap(0);
    private volatile ScheduledFuture<?> future;

    @SuppressWarnings("unchecked")
    public EntityTickLoop(
            RuntimeEntityMapping<?> mapping,
            TopicResolver topicResolver,
            SyncEventPublisher publisher,
            EntityStatsTracker statsTracker,
            EngineConfig config,
            ScheduledExecutorService scheduler) {
        this.mapping = (RuntimeEntityMapping<Object>) mapping;
        this.entityName = mapping.entityName();
        this.topicResolver = topicResolver;
        this.publisher = publisher;
        this.statsTracker = statsTracker;
        this.config = config;
        this.scheduler = scheduler;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("entity '{}' tick loop already started — ignoring", entityName);
            return;
        }
        Runnable tick = SafeRunnable.wrap(this::guardedTick, log);
        // First tick fires after one interval — keep boot quiet, then settle into cadence.
        future = scheduler.scheduleWithFixedDelay(
                tick, config.tickIntervalSeconds(), config.tickIntervalSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        running.set(false);
        ScheduledFuture<?> handle = future;
        if (handle != null) {
            handle.cancel(false);
        }
        future = null;
    }

    void guardedTick() {
        if (!running.get()) {
            return;
        }
        if (!ticking.compareAndSet(false, true)) {
            log.warn(
                    "entity '{}' tick still running — skipping scheduled tick (consider raising tick-interval)",
                    entityName);
            return;
        }
        try {
            tick();
        } finally {
            ticking.set(false);
        }
    }

    void tick() {
        long start = System.currentTimeMillis();
        String topic = topicResolver.resolveTopic(entityName);
        if (topic == null) {
            log.warn("no runtime topic for entity '{}', skipping tick", entityName);
            statsTracker.recordCycleResult(entityName, CycleResult.degraded(System.currentTimeMillis() - start));
            return;
        }

        Long2LongMap currentSnapshot = newHashMap(0);
        Long2ObjectMap<Object> dtosByPk = new Long2ObjectOpenHashMap<Object>();
        Iterable<RuntimeRow<Object>> rows;
        try {
            rows = mapping.snapshot();
        } catch (Throwable t) {
            log.error(
                    "entity '{}' snapshot threw {}: {}",
                    entityName,
                    t.getClass().getName(),
                    t.getMessage());
            statsTracker.recordCycleResult(entityName, CycleResult.degraded(System.currentTimeMillis() - start));
            return;
        }
        if (rows == null) {
            log.warn("entity '{}' snapshot returned null — treating as empty", entityName);
            rows = java.util.Collections.emptyList();
        }
        try {
            for (RuntimeRow<Object> row : rows) {
                if (row == null) continue;
                long pk = row.getPk();
                Object dto = row.getDto();
                long hash;
                try {
                    hash = mapping.hash(dto);
                } catch (Throwable t) {
                    log.warn(
                            "entity '{}' hash(pk={}) threw {} — skipping row",
                            entityName,
                            pk,
                            t.getClass().getName());
                    continue;
                }
                currentSnapshot.put(pk, hash);
                dtosByPk.put(pk, dto);
            }
        } catch (Throwable iterFailure) {
            log.warn(
                    "entity '{}' snapshot iteration failed: {}",
                    entityName,
                    iterFailure.getClass().getName(),
                    iterFailure);
            statsTracker.recordCycleResult(entityName, CycleResult.degraded(System.currentTimeMillis() - start));
            return;
        }

        Long2LongMap prev = prevSnapshot;
        Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>();
        // Pre-size to currentSnapshot — final occupancy cannot exceed it.
        Long2LongMap nextPrev = newHashMap(currentSnapshot.size());
        long created = 0L;
        long updated = 0L;

        // Single pass: classify each pk as NEW / CHANGED / unchanged via one
        // prev.get() (MISSING_HASH sentinel disambiguates absence from hash=0).
        LongIterator it = currentSnapshot.keySet().iterator();
        while (it.hasNext()) {
            long pk = it.nextLong();
            long currentHash = currentSnapshot.get(pk);
            long prevHash = prev.get(pk);
            String op;
            if (prevHash == MISSING_HASH) {
                op = SyncEventPublisher.OP_CREATED;
                created++;
            } else if (prevHash != currentHash) {
                op = SyncEventPublisher.OP_UPDATED;
                updated++;
            } else {
                nextPrev.put(pk, currentHash);
                continue;
            }
            CompletableFuture<RecordMetadata> f = publisher.publish(mapping, op, pk, dtosByPk.get(pk), topic);
            inFlight.put(pk, f);
        }

        long[] ackResult = walkInFlight(inFlight, prev, nextPrev, currentSnapshot);
        long failedAcks = ackResult[0];
        long timedOutAcks = ackResult[1];

        prevSnapshot = nextPrev;

        long duration = System.currentTimeMillis() - start;
        long rowCount = currentSnapshot.size();
        CycleResult result;
        if (failedAcks + timedOutAcks > 0L) {
            result = CycleResult.degraded(duration, created, updated, rowCount, failedAcks, timedOutAcks);
        } else {
            result = CycleResult.healthy(duration, created, updated, rowCount);
        }
        statsTracker.recordCycleResult(entityName, result);
    }

    /**
     * Drains already-done futures cheaply on the first pass, then deadline-waits
     * pending ones for the remainder of {@code publishFlushSeconds}. Failed
     * publishes carry the previous hash forward so replay happens next tick
     * (at-least-once contract).
     */
    private long[] walkInFlight(
            Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight,
            Long2LongMap prev,
            Long2LongMap nextPrev,
            Long2LongMap currentSnapshot) {
        long failedAcks = 0L;
        long timedOutAcks = 0L;
        if (inFlight.isEmpty()) {
            return new long[] {0L, 0L};
        }

        // Pass 1: drain already-done — no blocking, just classify.
        Long2ObjectMap<CompletableFuture<RecordMetadata>> pending =
                new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>();
        ObjectIterator<Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>>> it =
                inFlight.long2ObjectEntrySet().iterator();
        while (it.hasNext()) {
            Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>> e = it.next();
            long pk = e.getLongKey();
            CompletableFuture<RecordMetadata> f = e.getValue();
            if (!f.isDone()) {
                pending.put(pk, f);
                continue;
            }
            if (f.isCompletedExceptionally()) {
                failedAcks++;
                carryPrev(prev, nextPrev, pk);
            } else {
                nextPrev.put(pk, currentSnapshot.get(pk));
            }
        }

        // Pass 2: deadline-bounded wait for the rest.
        if (!pending.isEmpty()) {
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.publishFlushSeconds());
            ObjectIterator<Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>>> pendIt =
                    pending.long2ObjectEntrySet().iterator();
            while (pendIt.hasNext()) {
                Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>> e = pendIt.next();
                long pk = e.getLongKey();
                CompletableFuture<RecordMetadata> f = e.getValue();
                long remaining = Math.max(0L, deadlineNanos - System.nanoTime());
                try {
                    f.get(remaining, TimeUnit.NANOSECONDS);
                    nextPrev.put(pk, currentSnapshot.get(pk));
                } catch (TimeoutException timeout) {
                    timedOutAcks++;
                    carryPrev(prev, nextPrev, pk);
                } catch (InterruptedException ie) {
                    // Shutdown signal — stop walking and let the scheduler tear down.
                    Thread.currentThread().interrupt();
                    timedOutAcks++;
                    carryPrev(prev, nextPrev, pk);
                    break;
                } catch (ExecutionException ex) {
                    failedAcks++;
                    carryPrev(prev, nextPrev, pk);
                } catch (Throwable t) {
                    failedAcks++;
                    carryPrev(prev, nextPrev, pk);
                }
            }
        }
        if (failedAcks > 0L) {
            log.warn("entity '{}' {} publish failures — replay next tick", entityName, failedAcks);
        }
        if (timedOutAcks > 0L) {
            log.warn(
                    "entity '{}' {} publishes still pending past flush deadline ({}s) — replay next tick",
                    entityName,
                    timedOutAcks,
                    config.publishFlushSeconds());
        }
        return new long[] {failedAcks, timedOutAcks};
    }

    private static void carryPrev(Long2LongMap prev, Long2LongMap nextPrev, long pk) {
        long prior = prev.get(pk);
        if (prior != MISSING_HASH) {
            nextPrev.put(pk, prior);
        }
    }

    private static Long2LongOpenHashMap newHashMap(int initialCapacity) {
        Long2LongOpenHashMap map =
                initialCapacity > 0 ? new Long2LongOpenHashMap(initialCapacity) : new Long2LongOpenHashMap();
        map.defaultReturnValue(MISSING_HASH);
        return map;
    }

    // Test seam — exposes the post-tick snapshot keys.
    List<Long> currentSnapshotKeysForTesting() {
        Long2LongMap snap = prevSnapshot;
        List<Long> result = new ArrayList<Long>(snap.size());
        LongIterator it = snap.keySet().iterator();
        while (it.hasNext()) {
            result.add(it.nextLong());
        }
        return result;
    }
}
