package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.events.sync.ResyncCompletedEvent;
import app.l2nx.gs.adapter.api.kafka.ops.model.EntityState;
import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/**
 * Per-entity force-resync bookkeeping: enqueue from any thread, drain/invalidate/completion only on the entity's cycle thread.
 * A whole-entity request absorbs queued PK sets (dropping ghost sentinels; the concurrent FULL operation's platform sweep covers ghosts);
 * drained {@code resyncId}s stay in-flight with their original drain time until the first fully successful cycle (HEALTHY, zero failed, zero pending publishes).
 */
final class ResyncCoordinator {

    private static final NxLog log = NxLogFactory.getLogger(ResyncCoordinator.class);

    private final NxEvents events;
    private final Map<String, Pending> pendingByEntity = new ConcurrentHashMap<String, Pending>();
    // Single-writer on the cycle thread; ConcurrentHashMap only for the cross-entity map structure.
    private final Map<String, List<InFlight>> inFlightByEntity = new ConcurrentHashMap<String, List<InFlight>>();

    ResyncCoordinator(NxEvents events) {
        this.events = events;
    }

    void enqueueAll(UUID resyncId, String entityName) {
        Pending pending = pendingOf(entityName);
        synchronized (pending) {
            pending.all = true;
            pending.pks.clear();
            // invalidate-all subsumes queued no-event per-pk requests.
            pending.noEventPks.clear();
            pending.resyncIds.add(resyncId);
        }
    }

    void enqueuePks(UUID resyncId, String entityName, LongSet pks) {
        Pending pending = pendingOf(entityName);
        synchronized (pending) {
            if (!pending.all) {
                pending.pks.addAll(pks);
            }
            pending.resyncIds.add(resyncId);
        }
    }

    /** No-event per-command pk-republish: invalidated on drain but never tracked as {@link InFlight}, so no {@link ResyncCompletedEvent}. */
    void enqueueNoEventPks(String entityName, LongSet pks) {
        Pending pending = pendingOf(entityName);
        synchronized (pending) {
            if (!pending.all) {
                pending.noEventPks.addAll(pks);
            }
        }
    }

    boolean hasPending(String entityName) {
        Pending pending = pendingByEntity.get(entityName);
        if (pending == null) {
            return false;
        }
        synchronized (pending) {
            return !pending.resyncIds.isEmpty() || !pending.noEventPks.isEmpty();
        }
    }

    /**
     * Runs before window planning so inserted sentinels extend the PK envelope this cycle; cycle thread only.
     * {@code targetedOnly} is true when something was drained and no whole-entity request was pending; then {@code targetedPks} is the bounded set for {@code IN(...)}.
     */
    DrainResult drainAndInvalidate(String entityName, SnapshotStore snapshot) {
        Pending pending = pendingByEntity.get(entityName);
        if (pending == null) {
            return DrainResult.EMPTY;
        }
        boolean all;
        LongOpenHashSet pks;
        LongOpenHashSet noEventPks;
        List<UUID> drainedIds;
        synchronized (pending) {
            if (pending.resyncIds.isEmpty() && pending.noEventPks.isEmpty()) {
                return DrainResult.EMPTY;
            }
            all = pending.all;
            pks = pending.pks.isEmpty() ? null : new LongOpenHashSet(pending.pks);
            noEventPks = pending.noEventPks.isEmpty() ? null : new LongOpenHashSet(pending.noEventPks);
            drainedIds = new ArrayList<UUID>(pending.resyncIds);
            pending.all = false;
            pending.pks.clear();
            pending.noEventPks.clear();
            pending.resyncIds.clear();
        }
        if (all) {
            snapshot.invalidateAll(entityName);
            log.info(
                    "Force resync: invalidated all {} snapshot entries of entity {} (resyncIds={})",
                    snapshot.sizeOf(entityName),
                    entityName,
                    drainedIds);
        } else if (pks != null) {
            LongIterator it = pks.iterator();
            while (it.hasNext()) {
                snapshot.invalidate(entityName, it.nextLong());
            }
            log.info(
                    "Force resync: invalidated {} rows of entity {} (resyncIds={})",
                    pks.size(),
                    entityName,
                    drainedIds);
        }
        // No-event PKs record no InFlight, so no ResyncCompletedEvent; already perturbed if an all-request was
        // absorbed.
        if (!all && noEventPks != null) {
            LongIterator it = noEventPks.iterator();
            while (it.hasNext()) {
                snapshot.invalidate(entityName, it.nextLong());
            }
            log.debug(
                    "No-event resync: invalidated {} rows of entity {} (per-command, no completion event)",
                    noEventPks.size(),
                    entityName);
        }
        if (!drainedIds.isEmpty()) {
            Instant cycleStartedAt = Instant.now();
            List<InFlight> inFlight = inFlightOf(entityName);
            for (UUID resyncId : drainedIds) {
                if (!containsId(inFlight, resyncId)) {
                    inFlight.add(new InFlight(resyncId, cycleStartedAt));
                }
            }
        }
        if (all) {
            return DrainResult.EMPTY;
        }
        // Empty union -> null (no fast-path).
        LongOpenHashSet union = new LongOpenHashSet();
        if (pks != null) {
            union.addAll(pks);
        }
        if (noEventPks != null) {
            union.addAll(noEventPks);
        }
        return new DrainResult(union.isEmpty() ? null : union);
    }

    /** {@code targetedOnly} is true only when something was drained and no whole-entity request was pending; {@code targetedPks} is then the bounded set. */
    static final class DrainResult {
        static final DrainResult EMPTY = new DrainResult(null);

        @Nullable
        private final LongSet targetedPks;

        DrainResult(@Nullable LongSet targetedPks) {
            this.targetedPks = targetedPks;
        }

        boolean targetedOnly() {
            return targetedPks != null;
        }

        @Nullable
        LongSet targetedPks() {
            return targetedPks;
        }
    }

    /**
     * Emits one {@link ResyncCompletedEvent} per in-flight id after a fully successful cycle; otherwise keeps the list
     * (un-acked rows keep their perturbed hash, so the retry cycle re-publishes them). Cycle thread only.
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
            log.info(
                    "Force resync: entity {} cycle not fully successful (state={}, failed={}, pending={})"
                            + " — completion deferred for resyncIds={}",
                    entityName,
                    result.state(),
                    result.failedPublishes(),
                    result.pendingPublishes(),
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
            log.info(
                    "Force resync completed: entity={}, resyncId={}, cycleStartedAt={}",
                    entityName,
                    entry.resyncId,
                    entry.cycleStartedAt);
        }
        inFlight.clear();
    }

    /** Engine stop path; resync requests are deliberately not crash-durable. */
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
        final LongOpenHashSet noEventPks = new LongOpenHashSet();
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
