package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.events.sync.ResyncCompletedEvent;
import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.spi.NxEvents;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-entity force-resync bookkeeping for {@link CdcEngine}: thread-safe
 * pending-request enqueue from any thread, drain + snapshot invalidation +
 * completion emission strictly on the entity's cycle thread.
 *
 * <p>Merge semantics: a whole-entity request absorbs any queued PK sets; PK
 * sets union. A whole-entity absorb discards queued ghost-PK sentinels (PKs
 * the snapshot never had) by design — invalidate-all only perturbs stored
 * entries, so ghost convergence for the absorbed request relies on the
 * concurrent FULL operation's platform sweep. Drained {@code resyncId}s move
 * to a per-entity in-flight list
 * stamped with the drain time ({@code cycleStartedAt}); the list survives
 * non-successful cycles (the original drain time is kept) and is emitted +
 * cleared after the first FULLY successful cycle — HEALTHY state AND zero
 * failed AND zero still-pending publishes.</p>
 */
final class ResyncCoordinator {

    private static final NxLog log = NxLogFactory.getLogger(ResyncCoordinator.class);

    private final NxEvents events;
    private final Map<String, Pending> pendingByEntity = new ConcurrentHashMap<String, Pending>();
    // Mutated only on the entity's cycle thread (under the engine's ticking guard);
    // ConcurrentHashMap only for the cross-entity map structure itself.
    private final Map<String, List<InFlight>> inFlightByEntity =
            new ConcurrentHashMap<String, List<InFlight>>();

    ResyncCoordinator(NxEvents events) {
        this.events = events;
    }

    /**
     * Enqueue a whole-entity resync. Any thread; non-blocking; absorbs
     * previously queued PK sets for the entity.
     */
    void enqueueAll(UUID resyncId, String entityName) {
        Pending pending = pendingOf(entityName);
        synchronized (pending) {
            pending.all = true;
            pending.pks.clear();
            pending.resyncIds.add(resyncId);
        }
    }

    /**
     * Enqueue a selected-rows resync. Any thread; non-blocking; unions with
     * previously queued PK sets; a queued whole-entity request absorbs it.
     */
    void enqueuePks(UUID resyncId, String entityName, LongSet pks) {
        Pending pending = pendingOf(entityName);
        synchronized (pending) {
            if (!pending.all) {
                pending.pks.addAll(pks);
            }
            pending.resyncIds.add(resyncId);
        }
    }

    boolean hasPending(String entityName) {
        Pending pending = pendingByEntity.get(entityName);
        if (pending == null) {
            return false;
        }
        synchronized (pending) {
            return !pending.resyncIds.isEmpty();
        }
    }

    /**
     * Drain the entity's pending requests and apply the invalidations to the
     * snapshot. Cycle thread only — runs at the top of the guarded cycle,
     * BEFORE the task plans windows, so inserted sentinels extend the
     * snapshot's PK envelope and are scanned this cycle.
     */
    void drainAndInvalidate(String entityName, SnapshotStore snapshot) {
        Pending pending = pendingByEntity.get(entityName);
        if (pending == null) {
            return;
        }
        boolean all;
        LongOpenHashSet pks;
        List<UUID> drainedIds;
        synchronized (pending) {
            if (pending.resyncIds.isEmpty()) {
                return;
            }
            all = pending.all;
            pks = pending.pks.isEmpty() ? null : new LongOpenHashSet(pending.pks);
            drainedIds = new ArrayList<UUID>(pending.resyncIds);
            pending.all = false;
            pending.pks.clear();
            pending.resyncIds.clear();
        }
        if (all) {
            snapshot.invalidateAll(entityName);
            log.info("Force resync: invalidated all {} snapshot entries of entity {} (resyncIds={})",
                    snapshot.sizeOf(entityName), entityName, drainedIds);
        } else if (pks != null) {
            LongIterator it = pks.iterator();
            while (it.hasNext()) {
                snapshot.invalidate(entityName, it.nextLong());
            }
            log.info("Force resync: invalidated {} rows of entity {} (resyncIds={})",
                    pks.size(), entityName, drainedIds);
        }
        Instant cycleStartedAt = Instant.now();
        List<InFlight> inFlight = inFlightOf(entityName);
        for (UUID resyncId : drainedIds) {
            if (!containsId(inFlight, resyncId)) {
                inFlight.add(new InFlight(resyncId, cycleStartedAt));
            }
        }
    }

    /**
     * Completion gate, cycle thread only — runs right after the cycle whose
     * drain populated the in-flight list. Emits one {@link ResyncCompletedEvent}
     * per in-flight {@code resyncId} when the cycle was fully successful;
     * otherwise keeps the list for the retry cycle (un-acked rows keep their
     * perturbed hash, so the next cycle re-publishes them).
     */
    void onCycleResult(String entityName, CycleResult result) {
        List<InFlight> inFlight = inFlightByEntity.get(entityName);
        if (inFlight == null || inFlight.isEmpty()) {
            return;
        }
        boolean fullySuccessful = result.state() == EntityState.HEALTHY
                && result.failedPublishes() == 0L
                && result.pendingPublishes() == 0L;
        if (!fullySuccessful) {
            log.info("Force resync: entity {} cycle not fully successful (state={}, failed={}, pending={})"
                            + " — completion deferred for resyncIds={}",
                    entityName, result.state(), result.failedPublishes(), result.pendingPublishes(),
                    idsOf(inFlight));
            return;
        }
        Instant completedAt = Instant.now();
        for (InFlight entry : inFlight) {
            events.publish(ResyncCompletedEvent.builder()
                    .eventId(UUIDv7.generate())
                    .resyncId(entry.resyncId)
                    .entityName(entityName)
                    .cycleStartedAt(entry.cycleStartedAt)
                    .completedAt(completedAt)
                    .build());
            log.info("Force resync completed: entity={}, resyncId={}, cycleStartedAt={}",
                    entityName, entry.resyncId, entry.cycleStartedAt);
        }
        inFlight.clear();
    }

    /**
     * Drops every pending request and in-flight id. Engine stop path —
     * matches the documented non-goal of crash-durable resync requests.
     */
    void clear() {
        pendingByEntity.clear();
        inFlightByEntity.clear();
    }

    private Pending pendingOf(String entityName) {
        Pending pending = pendingByEntity.get(entityName);
        if (pending == null) {
            pending = new Pending();
            Pending raced = pendingByEntity.putIfAbsent(entityName, pending);
            if (raced != null) {
                pending = raced;
            }
        }
        return pending;
    }

    private List<InFlight> inFlightOf(String entityName) {
        List<InFlight> list = inFlightByEntity.get(entityName);
        if (list == null) {
            list = new ArrayList<InFlight>();
            inFlightByEntity.put(entityName, list);
        }
        return list;
    }

    private static boolean containsId(List<InFlight> inFlight, UUID resyncId) {
        for (InFlight entry : inFlight) {
            if (entry.resyncId.equals(resyncId)) {
                return true;
            }
        }
        return false;
    }

    private static List<UUID> idsOf(List<InFlight> inFlight) {
        List<UUID> ids = new ArrayList<UUID>(inFlight.size());
        for (InFlight entry : inFlight) {
            ids.add(entry.resyncId);
        }
        return ids;
    }

    private static final class Pending {
        boolean all;
        final LongOpenHashSet pks = new LongOpenHashSet();
        final Set<UUID> resyncIds = new LinkedHashSet<UUID>();
    }

    private static final class InFlight {
        final UUID resyncId;
        final Instant cycleStartedAt;

        InFlight(UUID resyncId, Instant cycleStartedAt) {
            this.resyncId = resyncId;
            this.cycleStartedAt = cycleStartedAt;
        }
    }
}
