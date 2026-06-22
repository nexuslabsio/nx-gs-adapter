package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.ChangesSummary;
import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds the latest {@link EntityStats} per entity. Writers are
 * {@link EntityTickLoop}s; readers are heartbeat threads. Mirror of the
 * {@code db-sync} tracker — same heartbeat surface so operators see both
 * modules side-by-side under {@code ModuleStatus.stats.entities}.
 */
public final class EntityStatsTracker {

    private final Map<String, EntityStats> latest = new ConcurrentHashMap<String, EntityStats>();
    private final Map<String, AtomicInteger> errorCounters = new ConcurrentHashMap<String, AtomicInteger>();
    private final Map<String, AtomicLong> failedAcks = new ConcurrentHashMap<String, AtomicLong>();
    private final Map<String, AtomicLong> timedOutAcks = new ConcurrentHashMap<String, AtomicLong>();
    private final Map<String, Integer> entityOrder = new ConcurrentHashMap<String, Integer>();
    private final AtomicInteger orderCursor = new AtomicInteger(0);

    public void recordCycleResult(String entityName, CycleResult cycle) {
        AtomicInteger counter = errorCounters.computeIfAbsent(entityName, k -> new AtomicInteger(0));
        int errors;
        if (cycle.state() == EntityState.DEGRADED) {
            errors = counter.incrementAndGet();
        } else {
            counter.set(0);
            errors = 0;
        }
        failedAcks.computeIfAbsent(entityName, k -> new AtomicLong(0L)).set(cycle.failedAcks());
        timedOutAcks.computeIfAbsent(entityName, k -> new AtomicLong(0L)).set(cycle.timedOutAcks());
        EntityStats stats = EntityStats.builder()
                .name(entityName)
                .state(cycle.state())
                .rowCount(cycle.rowCount())
                .lastSyncEpochMs(System.currentTimeMillis())
                .lastCycleDurationMs(cycle.durationMs())
                .lastCycleChanges(ChangesSummary.builder()
                        .created(cycle.created())
                        .updated(cycle.updated())
                        .deleted(0L)
                        .build())
                .consecutiveErrors(errors)
                .build();
        entityOrder.computeIfAbsent(entityName, k -> orderCursor.getAndIncrement());
        latest.put(entityName, stats);
    }

    public List<EntityStats> currentStatuses() {
        if (latest.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map.Entry<String, Integer>> ordered = new ArrayList<Map.Entry<String, Integer>>(entityOrder.entrySet());
        ordered.sort((a, b) -> Integer.compare(a.getValue(), b.getValue()));
        List<EntityStats> result = new ArrayList<EntityStats>(ordered.size());
        for (Map.Entry<String, Integer> e : ordered) {
            EntityStats stats = latest.get(e.getKey());
            if (stats != null) {
                result.add(stats);
            }
        }
        return Collections.unmodifiableList(result);
    }

    public int consecutiveErrors(String entityName) {
        AtomicInteger counter = errorCounters.get(entityName);
        return counter == null ? 0 : counter.get();
    }

    public long failedAcks(String entityName) {
        AtomicLong v = failedAcks.get(entityName);
        return v == null ? 0L : v.get();
    }

    public long timedOutAcks(String entityName) {
        AtomicLong v = timedOutAcks.get(entityName);
        return v == null ? 0L : v.get();
    }

    public void clear() {
        latest.clear();
        errorCounters.clear();
        failedAcks.clear();
        timedOutAcks.clear();
        entityOrder.clear();
        orderCursor.set(0);
    }
}
