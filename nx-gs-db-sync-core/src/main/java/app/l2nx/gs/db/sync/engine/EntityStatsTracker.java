package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.ChangesSummary;
import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the latest {@link EntityStats} per entity. Writers are
 * {@link EntitySyncTask}s (one thread per entity); readers are heartbeat
 * threads. Per-entity stats are stored in a {@link ConcurrentHashMap} so a
 * reader walking the keys observes the latest committed value for each entity
 * without blocking writers.
 *
 * <p>{@link #recordCycleResult} also drives the {@code consecutiveErrors}
 * counter: increments on {@link EntityState#DEGRADED}, resets to 0 on
 * {@link EntityState#HEALTHY}.</p>
 */
public final class EntityStatsTracker {

    private final Map<String, EntityStats> latest = new ConcurrentHashMap<String, EntityStats>();
    private final Map<String, AtomicInteger> errorCounters = new ConcurrentHashMap<String, AtomicInteger>();
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
        EntityStats stats = EntityStats.builder()
                .name(entityName)
                .state(cycle.state())
                .rowCount(cycle.rowCount())
                .lastSyncEpochMs(System.currentTimeMillis())
                .lastCycleDurationMs(cycle.durationMs())
                .lastCycleChanges(ChangesSummary.builder()
                        .created(cycle.created())
                        .updated(cycle.updated())
                        .deleted(cycle.deleted())
                        .build())
                .consecutiveErrors(errors)
                .build();
        // Order slot first, then stats — a reader between the two writes either misses
        // the entity entirely (acceptable) or sees a complete (order, stats) pair.
        // The reverse would let a reader observe stats with no order entry and silently
        // drop the entity from the snapshot.
        entityOrder.computeIfAbsent(entityName, k -> orderCursor.getAndIncrement());
        latest.put(entityName, stats);
    }

    /**
     * Snapshot of every recorded entity, ordered by the entity's first-record
     * insertion order. Returns an immutable list; entries themselves are
     * immutable {@link EntityStats}.
     */
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

    public void clear() {
        latest.clear();
        errorCounters.clear();
        entityOrder.clear();
        orderCursor.set(0);
    }
}
