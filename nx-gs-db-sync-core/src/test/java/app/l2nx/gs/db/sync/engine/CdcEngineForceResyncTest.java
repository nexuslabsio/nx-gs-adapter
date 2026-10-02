package app.l2nx.gs.db.sync.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import app.l2nx.gs.adapter.api.kafka.events.sync.ResyncCompletedEvent;
import app.l2nx.gs.adapter.api.kafka.ops.model.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.model.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.model.PoolStats;
import app.l2nx.gs.adapter.api.spi.provider.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.persist.SnapshotPersistence;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Force-resync wiring: drain-before-cycle ordering, completion gating, mid-cycle re-submit; an empty host DB makes invalidated entries diff to DELETED. */
class CdcEngineForceResyncTest {

    private static final long PK = 1L;
    private static final UUID RESYNC_ID = UUID.fromString("018f0000-0000-7000-8000-0000000000cc");
    private static final RecordMetadata META = new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);

    private final SnapshotStore snapshot = new SnapshotStore();
    private final CapturingSource source = new CapturingSource(snapshot);
    private final List<Object> published = Collections.synchronizedList(new ArrayList<Object>());
    private final AtomicBoolean failPublishes = new AtomicBoolean(false);
    private final AtomicInteger checkpoints = new AtomicInteger();
    private final EntityStatsTracker statsTracker = new EntityStatsTracker();

    private CdcEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void requestForceResync_shouldReturnFalse_whenEntityUnknown() {
        engine = buildEngine();
        engine.start();

        assertFalse(engine.requestForceResync(RESYNC_ID, "nope"));
        assertFalse(engine.requestForceResync(RESYNC_ID, "nope", pks(PK)));
    }

    @Test
    void requestForceResync_shouldReturnFalse_beforeStart() {
        engine = buildEngine();

        assertFalse(engine.requestForceResync(RESYNC_ID, "clan"));
    }

    @Test
    void requestForceResync_shouldInvalidateBeforeCycle_andEmitCompletionAfterFullySuccessfulCycle() {
        snapshot.putCrc("clan", PK, 100);
        engine = buildEngine();
        engine.start();

        assertTrue(engine.requestForceResync(RESYNC_ID, "clan", pks(PK)));

        await(() -> published.size() == 1);
        // Drain ran before the cycle borrowed its connection.
        int crcSeenByCycle = source.crcAtBorrow.get(0);
        assertNotEquals(100, crcSeenByCycle);
        assertNotEquals(Phase1Hasher.MISSING_HASH, crcSeenByCycle);

        ResyncCompletedEvent completed = (ResyncCompletedEvent) published.get(0);
        assertEquals(RESYNC_ID, completed.getResyncId());
        assertEquals("clan", completed.getEntityName());
        assertFalse(completed.getCompletedAt().isBefore(completed.getCycleStartedAt()));
        assertFalse(snapshot.containsCrc("clan", PK));
    }

    @Test
    void requestForceResync_shouldDeferCompletion_untilCycleWithoutPublishFailures() {
        snapshot.putCrc("clan", PK, 100);
        failPublishes.set(true);
        engine = buildEngine();
        engine.start();

        assertTrue(engine.requestForceResync(RESYNC_ID, "clan", pks(PK)));

        await(() -> checkpoints.get() >= 1);
        assertTrue(published.isEmpty(), "failed publish cycle must not emit completion");

        failPublishes.set(false);
        engine.triggerEntityNow("clan");

        await(() -> published.size() == 1);
        assertEquals(RESYNC_ID, ((ResyncCompletedEvent) published.get(0)).getResyncId());

        int cyclesSoFar = checkpoints.get();
        engine.triggerEntityNow("clan");
        await(() -> checkpoints.get() > cyclesSoFar);
        assertEquals(1, published.size(), "completion must be emitted exactly once");
    }

    @Test
    void requestPkRepublishNoEvent_shouldForceRepublishPk_withoutEmittingCompletionEvent() {
        snapshot.putCrc("clan", PK, 100);
        engine = buildEngine();
        engine.start();

        engine.requestPkRepublishNoEvent("clan", pks(PK));

        await(() -> !snapshot.containsCrc("clan", PK));
        int crcSeenByCycle = source.crcAtBorrow.get(0);
        assertNotEquals(100, crcSeenByCycle);
        assertNotEquals(Phase1Hasher.MISSING_HASH, crcSeenByCycle);

        // Internal per-command resync: no ResyncCompletedEvent ever lands.
        engine.triggerEntityNow("clan");
        await(() -> checkpoints.get() >= 2);
        assertTrue(published.isEmpty(), "no-event republish must not emit any completion event");
    }

    @Test
    void requestPkRepublishNoEvent_shouldBeNoOp_whenEntityUnknownOrPksEmpty() {
        engine = buildEngine();
        engine.start();

        engine.requestPkRepublishNoEvent("nope", pks(PK));
        engine.requestPkRepublishNoEvent("clan", pks());
        engine.requestPkRepublishNoEvent("clan", null);

        assertTrue(published.isEmpty());
    }

    @Test
    void runGuardedCycle_shouldResubmitImmediately_whenRequestLandsMidCycle() throws Exception {
        engine = buildEngine();
        engine.start();

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        source.enteredCycle = entered;
        source.releaseCycle = release;

        engine.triggerEntityNow("clan");
        assertTrue(entered.await(10, TimeUnit.SECONDS), "first cycle must reach the JDBC borrow");

        // Mid-cycle request: only the end-of-cycle re-submit can run it.
        assertTrue(engine.requestForceResync(RESYNC_ID, "clan", pks(PK)));
        release.countDown();

        await(() -> published.size() == 1);
        assertEquals(RESYNC_ID, ((ResyncCompletedEvent) published.get(0)).getResyncId());
    }

    @Test
    void runGuardedCycle_shouldRecordDegradedResultAndKeepTicking_whenGarbagePkExplodesWindowPlanning() {
        snapshot.putCrc("clan", PK, 100);
        // A garbage PK inflates the envelope; a whole-entity resync full-scans it and trips the plan-size cap (per-PK
        // would take the IN fast-path).
        snapshot.putCrc("clan", Long.MAX_VALUE / 2, 200);
        engine = buildEngine();
        engine.start();

        assertTrue(engine.requestForceResync(RESYNC_ID, "clan"));

        await(() -> entityState("clan") == EntityState.DEGRADED);
        assertTrue(published.isEmpty(), "an exploded cycle must not emit completion");
        assertEquals(0, checkpoints.get(), "a thrown cycle must skip the snapshot checkpoint");

        // Ticking guard released: a follow-up trigger still enters a cycle.
        int cyclesSoFar = source.crcAtBorrow.size();
        engine.triggerEntityNow("clan");
        await(() -> source.crcAtBorrow.size() > cyclesSoFar);
    }

    private EntityState entityState(String entity) {
        for (EntityStats stats : statsTracker.currentStatuses()) {
            if (entity.equals(stats.getName())) {
                return stats.getState();
            }
        }
        return null;
    }

    private CdcEngine buildEngine() {
        KafkaSender sender = (topic, key, value, callback) -> {
            if (failPublishes.get()) {
                callback.onCompletion(null, new RuntimeException("publish boom"));
            } else {
                callback.onCompletion(META, null);
            }
        };
        SnapshotPersistence persistence = new SnapshotPersistence() {
            @Override
            public void load(SnapshotStore target) {}

            @Override
            public void checkpoint(String entityName, SnapshotStore src) {
                checkpoints.incrementAndGet();
            }

            @Override
            public void flushAll(SnapshotStore src) {}

            @Override
            public void close() {}
        };
        return new CdcEngine(
                "test",
                Collections.singletonList(TestMappings.clanOnly()),
                source,
                snapshot,
                persistence,
                new EngineConfig(3600, 500_000, 5, 2),
                entity -> "test.gs.sync.clans",
                new SyncEventPublisher(sender),
                statsTracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null,
                RecordingNxEvents.into(published));
    }

    private static LongOpenHashSet pks(long... values) {
        LongOpenHashSet set = new LongOpenHashSet();
        for (long v : values) {
            set.add(v);
        }
        return set;
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("interrupted while awaiting condition");
            }
        }
        fail("condition not met within 10s");
    }

    /** Records the snapshot CRC of {@link #PK} at each borrow and can gate the first borrow to inject a request mid-cycle. */
    private static final class CapturingSource implements JdbcConnectionSource {
        final SnapshotStore snapshot;
        final List<Integer> crcAtBorrow = Collections.synchronizedList(new ArrayList<Integer>());
        volatile CountDownLatch enteredCycle;
        volatile CountDownLatch releaseCycle;

        CapturingSource(SnapshotStore snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public String name() {
            return "capturing";
        }

        @Override
        public Connection getConnection() {
            crcAtBorrow.add(snapshot.getCrc("clan", PK));
            CountDownLatch entered = enteredCycle;
            if (entered != null) {
                enteredCycle = null;
                entered.countDown();
            }
            CountDownLatch release = releaseCycle;
            if (release != null) {
                releaseCycle = null;
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            return mock(Connection.class, RETURNS_DEEP_STUBS);
        }

        @Override
        public Optional<PoolStats> stats() {
            return Optional.empty();
        }
    }
}
