package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.events.sync.ResyncCompletedEvent;
import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ResyncCoordinatorTest {

    private final List<Object> published = new ArrayList<Object>();
    private final ResyncCoordinator coordinator = new ResyncCoordinator(published::add);
    private final SnapshotStore snapshot = new SnapshotStore();

    private static final UUID RESYNC_A = UUID.fromString("018f0000-0000-7000-8000-0000000000aa");
    private static final UUID RESYNC_B = UUID.fromString("018f0000-0000-7000-8000-0000000000bb");

    @Test
    void enqueuePks_shouldUnionPkSets_acrossRequests() {
        snapshot.putCrc("clan", 1L, 100);
        snapshot.putCrc("clan", 2L, 200);
        snapshot.putCrc("clan", 3L, 300);

        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.enqueuePks(RESYNC_B, "clan", pks(2L));
        coordinator.drainAndInvalidate("clan", snapshot);

        assertNotEquals(100, snapshot.getCrc("clan", 1L));
        assertNotEquals(200, snapshot.getCrc("clan", 2L));
        assertEquals(300, snapshot.getCrc("clan", 3L), "pk 3 was never requested");
    }

    @Test
    void enqueueAll_shouldAbsorbQueuedPkSets() {
        snapshot.putCrc("clan", 1L, 100);
        snapshot.putCrc("clan", 2L, 200);

        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.enqueueAll(RESYNC_B, "clan");
        coordinator.drainAndInvalidate("clan", snapshot);

        // Whole-entity invalidation perturbs in place — no sentinel inserts.
        assertEquals(2, snapshot.sizeOf("clan"));
        assertNotEquals(100, snapshot.getCrc("clan", 1L));
        assertNotEquals(200, snapshot.getCrc("clan", 2L));
    }

    @Test
    void enqueuePks_afterEnqueueAll_shouldBeAbsorbedByPendingWholeEntity() {
        snapshot.putCrc("clan", 1L, 100);

        coordinator.enqueueAll(RESYNC_A, "clan");
        coordinator.enqueuePks(RESYNC_B, "clan", pks(999L));
        coordinator.drainAndInvalidate("clan", snapshot);

        // The whole-entity request wins: no sentinel insert for pk 999.
        assertFalse(snapshot.containsCrc("clan", 999L));
        assertNotEquals(100, snapshot.getCrc("clan", 1L));
    }

    @Test
    void drainAndInvalidate_shouldInsertSentinels_forUnknownPks() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(7L, 8L));
        coordinator.drainAndInvalidate("clan", snapshot);

        assertTrue(snapshot.containsCrc("clan", 7L));
        assertTrue(snapshot.containsCrc("clan", 8L));
        assertNotEquals(Phase1Hasher.MISSING_HASH, snapshot.getCrc("clan", 7L));
    }

    @Test
    void drainAndInvalidate_shouldClearPending() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        assertTrue(coordinator.hasPending("clan"));

        coordinator.drainAndInvalidate("clan", snapshot);

        assertFalse(coordinator.hasPending("clan"));
    }

    @Test
    void onCycleResult_shouldEmitOneEventPerDrainedResyncId_whenFullySuccessful() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.enqueueAll(RESYNC_B, "clan");
        coordinator.drainAndInvalidate("clan", snapshot);

        coordinator.onCycleResult("clan", healthy());

        assertEquals(2, published.size());
        List<UUID> resyncIds = new ArrayList<UUID>();
        for (Object event : published) {
            ResyncCompletedEvent completed = (ResyncCompletedEvent) event;
            assertEquals("clan", completed.getEntityName());
            assertNotNull(completed.getEventId());
            assertNotNull(completed.getCycleStartedAt());
            assertFalse(completed.getCompletedAt().isBefore(completed.getCycleStartedAt()));
            resyncIds.add(completed.getResyncId());
        }
        assertTrue(resyncIds.contains(RESYNC_A));
        assertTrue(resyncIds.contains(RESYNC_B));
    }

    @Test
    void onCycleResult_shouldDeferEmission_whenStateDegraded() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.drainAndInvalidate("clan", snapshot);

        coordinator.onCycleResult("clan", CycleResult.degraded(1L));

        assertTrue(published.isEmpty());
    }

    @Test
    void onCycleResult_shouldDeferEmission_whenPublishesFailed() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.drainAndInvalidate("clan", snapshot);

        coordinator.onCycleResult("clan", withPublishOutcome(1L, 0L));

        assertTrue(published.isEmpty());
    }

    @Test
    void onCycleResult_shouldDeferEmission_whenPublishesStillPending() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.drainAndInvalidate("clan", snapshot);

        coordinator.onCycleResult("clan", withPublishOutcome(0L, 1L));

        assertTrue(published.isEmpty());
    }

    @Test
    void onCycleResult_shouldEmitExactlyOnce_afterFailedThenSuccessfulCycle() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.drainAndInvalidate("clan", snapshot);

        coordinator.onCycleResult("clan", withPublishOutcome(1L, 0L));
        assertTrue(published.isEmpty());

        coordinator.drainAndInvalidate("clan", snapshot); // nothing pending — no-op
        coordinator.onCycleResult("clan", healthy());
        assertEquals(1, published.size());
        assertEquals(RESYNC_A, ((ResyncCompletedEvent) published.get(0)).getResyncId());

        coordinator.onCycleResult("clan", healthy());
        assertEquals(1, published.size(), "in-flight list must clear after emission");
    }

    @Test
    void drainAndInvalidate_shouldKeepOriginalDrainTime_whenIdRedrainedAfterFailedCycle() throws Exception {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.drainAndInvalidate("clan", snapshot);
        coordinator.onCycleResult("clan", withPublishOutcome(1L, 0L));

        Thread.sleep(5L);
        Instant beforeSecondDrain = Instant.now();
        // Same resyncId re-enqueued (command redelivery) and re-drained.
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.drainAndInvalidate("clan", snapshot);
        coordinator.onCycleResult("clan", healthy());

        assertEquals(1, published.size());
        ResyncCompletedEvent completed = (ResyncCompletedEvent) published.get(0);
        // cycleStartedAt anchors the platform sweep — must stay the FIRST drain
        // time so rows published by the first (partially failed) cycle are not
        // swept as ghosts.
        assertTrue(completed.getCycleStartedAt().isBefore(beforeSecondDrain));
    }

    @Test
    void onCycleResult_shouldBeNoOp_whenNothingInFlight() {
        coordinator.onCycleResult("clan", healthy());

        assertTrue(published.isEmpty());
    }

    @Test
    void clear_shouldDropPendingAndInFlight() {
        coordinator.enqueuePks(RESYNC_A, "clan", pks(1L));
        coordinator.enqueueAll(RESYNC_B, "item");
        coordinator.drainAndInvalidate("clan", snapshot);

        coordinator.clear();

        assertFalse(coordinator.hasPending("item"));
        coordinator.onCycleResult("clan", healthy());
        assertTrue(published.isEmpty());
    }

    private static LongOpenHashSet pks(long... values) {
        LongOpenHashSet set = new LongOpenHashSet();
        for (long v : values) {
            set.add(v);
        }
        return set;
    }

    private static CycleResult healthy() {
        return withPublishOutcome(0L, 0L);
    }

    private static CycleResult withPublishOutcome(long failed, long pending) {
        return new CycleResult(EntityState.HEALTHY, 1L, 0L, 1L, 0L, 1L, failed, pending);
    }
}
