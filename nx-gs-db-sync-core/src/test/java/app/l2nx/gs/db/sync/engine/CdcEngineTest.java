package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.sync.db.ClanDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
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

    private static EntityMapping<ClanDto> clanMapping() {
        return TestMappings.clanOnly();
    }

    private static KafkaSender neverCalledSender() {
        return (topic, key, value, callback) -> {
            throw new AssertionError("KafkaSender must not be invoked when entity has no topic");
        };
    }
}
