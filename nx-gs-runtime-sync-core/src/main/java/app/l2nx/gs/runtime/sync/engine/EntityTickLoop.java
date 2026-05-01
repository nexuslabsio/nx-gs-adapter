package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeRow;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * One daemon loop per declared runtime entity. Each tick:
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
 *
 * <p>All exception handling at this boundary — host JVM threads must not see
 * exceptions out of runtime-sync (per spec R8).</p>
 */
public final class EntityTickLoop {

    private static final NxLog log = NxLogFactory.getLogger(EntityTickLoop.class);

    private final RuntimeEntityMapping<Object> mapping;
    private final TopicResolver topicResolver;
    private final SyncEventPublisher publisher;
    private final EntityStatsTracker statsTracker;
    private final EngineConfig config;
    private final ScheduledExecutorService scheduler;

    private volatile Long2LongMap prevSnapshot = new Long2LongOpenHashMap();
    private volatile ScheduledFuture<?> future;

    @SuppressWarnings("unchecked")
    public EntityTickLoop(RuntimeEntityMapping<?> mapping,
                          TopicResolver topicResolver,
                          SyncEventPublisher publisher,
                          EntityStatsTracker statsTracker,
                          EngineConfig config,
                          ScheduledExecutorService scheduler) {
        this.mapping = (RuntimeEntityMapping<Object>) mapping;
        this.topicResolver = topicResolver;
        this.publisher = publisher;
        this.statsTracker = statsTracker;
        this.config = config;
        this.scheduler = scheduler;
    }

    public void start() {
        Runnable tick = SafeRunnable.wrap(this::tick, log);
        // First tick fires after one full interval so the engine doesn't pile work onto
        // adapter bootstrap; subsequent ticks every tickIntervalSeconds.
        future = scheduler.scheduleWithFixedDelay(tick,
                config.tickIntervalSeconds(),
                config.tickIntervalSeconds(),
                TimeUnit.SECONDS);
    }

    public void stop() {
        ScheduledFuture<?> running = future;
        if (running != null) {
            running.cancel(false);
        }
        future = null;
    }

    void tick() {
        long start = System.currentTimeMillis();
        String topic = topicResolver.resolveTopic(mapping.entityName());
        if (topic == null) {
            log.warn("no runtime topic for entity '{}', skipping tick", mapping.entityName());
            statsTracker.recordCycleResult(mapping.entityName(),
                    CycleResult.degraded(System.currentTimeMillis() - start));
            return;
        }

        Long2LongMap currentSnapshot = new Long2LongOpenHashMap();
        Long2ObjectMap<Object> dtosByPk = new Long2ObjectOpenHashMap<Object>();
        Iterable<RuntimeRow<Object>> rows;
        try {
            rows = mapping.snapshot();
        } catch (Throwable t) {
            log.error("entity '{}' snapshot threw {}: {}",
                    mapping.entityName(), t.getClass().getName(), t.getMessage());
            statsTracker.recordCycleResult(mapping.entityName(),
                    CycleResult.degraded(System.currentTimeMillis() - start));
            return;
        }
        if (rows == null) {
            log.warn("entity '{}' snapshot returned null — treating as empty",
                    mapping.entityName());
            rows = java.util.Collections.emptyList();
        }
        for (RuntimeRow<Object> row : rows) {
            if (row == null) continue;
            long pk = row.getPk();
            Object dto = row.getDto();
            long hash;
            try {
                hash = mapping.hash(dto);
            } catch (Throwable t) {
                log.warn("entity '{}' hash(pk={}) threw {} — skipping row",
                        mapping.entityName(), pk, t.getClass().getName());
                continue;
            }
            currentSnapshot.put(pk, hash);
            dtosByPk.put(pk, dto);
        }

        Long2LongMap prev = prevSnapshot;
        Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>();
        // Pre-size to currentSnapshot — final occupancy cannot exceed it.
        Long2LongMap nextPrev = new Long2LongOpenHashMap(currentSnapshot.size());
        long created = 0L;
        long updated = 0L;

        // Single pass: classify each pk as NEW / CHANGED / unchanged.
        // - NEW / CHANGED → publish, future enters inFlight (advances nextPrev only on ack)
        // - unchanged     → carry hash forward into nextPrev directly (no Kafka traffic)
        LongIterator it = currentSnapshot.keySet().iterator();
        while (it.hasNext()) {
            long pk = it.nextLong();
            long currentHash = currentSnapshot.get(pk);
            String op;
            if (!prev.containsKey(pk)) {
                op = SyncEventPublisher.OP_CREATED;
                created++;
            } else if (prev.get(pk) != currentHash) {
                op = SyncEventPublisher.OP_UPDATED;
                updated++;
            } else {
                nextPrev.put(pk, currentHash);
                continue;
            }
            CompletableFuture<RecordMetadata> f = publisher.publish(
                    mapping, op, pk, dtosByPk.get(pk), topic);
            inFlight.put(pk, f);
        }

        // Walk acks: failed publishes leave prev untouched for that pk → replayed
        // on next tick (at-least-once).
        long flushDeadlineMs = System.currentTimeMillis() + config.publishFlushSeconds() * 1000L;
        ObjectIterator<Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>>> ackIt =
                inFlight.long2ObjectEntrySet().iterator();
        while (ackIt.hasNext()) {
            Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>> e = ackIt.next();
            long pk = e.getLongKey();
            CompletableFuture<RecordMetadata> f = e.getValue();
            long remaining = Math.max(0L, flushDeadlineMs - System.currentTimeMillis());
            try {
                f.get(remaining, TimeUnit.MILLISECONDS);
                nextPrev.put(pk, currentSnapshot.get(pk));
            } catch (TimeoutException timeout) {
                log.warn("entity '{}' publish timed out for pk={} — replay next tick",
                        mapping.entityName(), pk);
                if (prev.containsKey(pk)) {
                    nextPrev.put(pk, prev.get(pk));
                }
            } catch (Throwable t) {
                log.warn("entity '{}' publish failed for pk={}: {} — replay next tick",
                        mapping.entityName(), pk, t.getClass().getName());
                if (prev.containsKey(pk)) {
                    nextPrev.put(pk, prev.get(pk));
                }
            }
        }
        prevSnapshot = nextPrev;

        long duration = System.currentTimeMillis() - start;
        statsTracker.recordCycleResult(mapping.entityName(),
                new CycleResult(EntityState.HEALTHY, duration, created, updated, currentSnapshot.size()));
    }

    // Test seam — drives one tick on the calling thread.
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
