package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.spi.model.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.model.RuntimeRow;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.commons.hash.Fnv1a64;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class EntityTickLoop {

    /** Sentinel because fastutil's default return value collides with a legitimate hash of 0. */
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
            rows = Collections.emptyList();
        }
        try {
            for (RuntimeRow<Object> row : rows) {
                if (row == null) continue;
                long pk = row.getPk();
                Object dto = row.getDto();
                long hash;
                try {
                    hash = mapping.hash(dto);
                    if (row.getStateStamp() != 0L) {
                        hash = Fnv1a64.mix(hash, row.getStateStamp());
                    }
                    // a host-chosen stamp could land on the absence sentinel and republish the row as CREATED forever
                    if (hash == MISSING_HASH) {
                        hash++;
                    }
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
        Long2LongMap nextPrev = newHashMap(currentSnapshot.size());
        long created = 0L;
        long updated = 0L;

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
