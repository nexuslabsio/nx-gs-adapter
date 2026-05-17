package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.persist.NoopSnapshotPersistence;
import app.l2nx.gs.db.sync.engine.persist.SnapshotPersistence;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class CdcEngineTest {

    private CdcEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void start_shouldSeedDegradedStats_whenEntityHasNoTopic() {
        EntityStatsTracker tracker = new EntityStatsTracker();
        TopicResolver resolver = entity -> null; // no topics for any entity
        engine = new CdcEngine(
                "bohpts",
                Collections.singletonList(clanMapping()),
                mock(JdbcConnectionSource.class),
                new SnapshotStore(),
                NoopSnapshotPersistence.INSTANCE,
                new EngineConfig(60, 500_000, 10, 5),
                resolver,
                new SyncEventPublisher(neverCalledSender()),
                tracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null);

        // Replace the running scheduler tick path: stop immediately so the
        // periodic scheduler doesn't fire ticks under test.
        engine.start();
        engine.stop();

        List<EntityStats> snapshot = tracker.currentStatuses();
        assertEquals(1, snapshot.size());
        assertEquals("clan", snapshot.get(0).getName());
        assertEquals(EntityState.DEGRADED, snapshot.get(0).getState());
    }

    @Test
    void stop_shouldClearSnapshot() {
        SnapshotStore snapshot = new SnapshotStore();
        snapshot.putCrc("clan", 1L, 100);
        snapshot.putCrc("clan", 2L, 200);
        assertEquals(2, snapshot.sizeOf("clan"));

        engine = new CdcEngine(
                "bohpts",
                Collections.singletonList(clanMapping()),
                mock(JdbcConnectionSource.class),
                snapshot,
                NoopSnapshotPersistence.INSTANCE,
                new EngineConfig(3600, 500_000, 10, 5), // long delay so first tick doesn't fire
                entity -> null,
                new SyncEventPublisher(neverCalledSender()),
                new EntityStatsTracker(),
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null);

        engine.start();
        engine.stop();

        assertEquals(0, snapshot.sizeOf("clan"));
    }

    @Test
    void start_shouldBeIdempotent() {
        EntityStatsTracker tracker = new EntityStatsTracker();
        engine = new CdcEngine(
                "bohpts",
                Collections.singletonList(clanMapping()),
                mock(JdbcConnectionSource.class),
                new SnapshotStore(),
                NoopSnapshotPersistence.INSTANCE,
                new EngineConfig(3600, 500_000, 10, 5),
                entity -> null,
                new SyncEventPublisher(neverCalledSender()),
                tracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null);

        engine.start();
        engine.start(); // second call must be a no-op
        engine.stop();

        // Only one DEGRADED seed regardless of repeated start()
        List<EntityStats> snapshot = tracker.currentStatuses();
        assertNotNull(snapshot);
        assertTrue(snapshot.size() <= 1);
    }

    @Test
    void lifecycle_shouldLoadOnStartAndFlushPlusCloseOnStop() {
        RecordingPersistence recorder = new RecordingPersistence();
        SnapshotStore snapshot = new SnapshotStore();

        engine = new CdcEngine(
                "bohpts",
                Collections.singletonList(clanMapping()),
                mock(JdbcConnectionSource.class),
                snapshot,
                recorder,
                new EngineConfig(3600, 500_000, 10, 5),
                entity -> null,
                new SyncEventPublisher(neverCalledSender()),
                new EntityStatsTracker(),
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null);

        engine.start();
        assertEquals(1, recorder.loadCount, "load() must run exactly once on start");
        assertEquals(0, recorder.flushCount, "no flush during start");

        engine.stop();
        assertEquals(1, recorder.flushCount, "flushAll() must run on stop");
        assertEquals(1, recorder.closeCount, "close() must run on stop");
        assertEquals(1L, recorder.flushSeenSizeBeforeClear,
                "flushAll must observe load()-seeded content (runs before snapshot.clearAll)");
        assertEquals(0, snapshot.sizeOf("clan"),
                "snapshot must be cleared on stop, AFTER flushAll");
    }

    @Test
    void start_shouldSwallowPersistenceLoadFailure_andStillRun() {
        SnapshotPersistence boom = new SnapshotPersistence() {
            @Override
            public void load(SnapshotStore t) {
                throw new RuntimeException("boom from load");
            }

            @Override
            public void checkpoint(String e, SnapshotStore s) {
            }

            @Override
            public void flushAll(SnapshotStore s) {
            }

            @Override
            public void close() {
            }
        };

        EntityStatsTracker tracker = new EntityStatsTracker();
        engine = new CdcEngine(
                "bohpts",
                Collections.singletonList(clanMapping()),
                mock(JdbcConnectionSource.class),
                new SnapshotStore(),
                boom,
                new EngineConfig(3600, 500_000, 10, 5),
                entity -> null,
                new SyncEventPublisher(neverCalledSender()),
                tracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null);

        engine.start();
        engine.stop();
        // No assertion needed on payload — the point is that start/stop didn't throw.
    }

    private static EntityMapping<ClanDto> clanMapping() {
        return TestMappings.clanOnly();
    }

    private static final class RecordingPersistence implements SnapshotPersistence {
        int loadCount;
        int flushCount;
        int closeCount;
        long flushSeenSizeBeforeClear = -1L;

        @Override
        public void load(SnapshotStore target) {
            loadCount++;
            // Seed something so flushAll has work to do — verifies flushAll runs
            // BEFORE clearAll() wipes the store.
            target.putCrc("clan", 1L, 999);
        }

        @Override
        public void checkpoint(String entityName, SnapshotStore source) {
        }

        @Override
        public void flushAll(SnapshotStore source) {
            flushCount++;
            flushSeenSizeBeforeClear = source.sizeOf("clan");
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    private static KafkaSender neverCalledSender() {
        return (topic, key, value, callback) -> {
            throw new AssertionError("KafkaSender must not be invoked when entity has no topic");
        };
    }
}
