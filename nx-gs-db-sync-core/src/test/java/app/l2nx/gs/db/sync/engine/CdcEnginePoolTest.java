package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.persist.NoopSnapshotPersistence;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcEnginePoolTest {

    private CdcEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
            engine = null;
        }
    }

    @Test
    void resolvePoolSize_shouldRespectConfiguredWorkersWhenPositive() {
        assertEquals(4, CdcEngine.resolvePoolSize(4, 8));
        assertEquals(1, CdcEngine.resolvePoolSize(1, 8));
    }

    @Test
    void resolvePoolSize_shouldComputeDefault_whenWorkersZero() {
        int sized = CdcEngine.resolvePoolSize(0, 8);
        assertTrue(sized >= 2, "default pool size must be at least 2");
    }

    @Test
    void sharedPool_shouldTickAllEntitiesWithoutStarvation() throws Exception {
        // Two entities sharing the same pool: both ticks must complete (recorded
        // as DEGRADED because the borrow fails). Without a shared pool with
        // worker capacity for both, the second entity would never tick.
        EntityMapping<ClanDto> mapping1 = TestMappings.clanOnly();
        EntityMapping<?> mapping2 = renameEntity(TestMappings.clanOnly(), "alpha");

        JdbcConnectionSource src = new JdbcConnectionSource() {
            @Override
            public String name() {
                return "stub";
            }

            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("pool-test never connects");
            }

            @Override
            public Optional<PoolStats> stats() {
                return Optional.empty();
            }
        };

        EntityStatsTracker tracker = new EntityStatsTracker();
        TopicResolver topicResolver = entity -> "topic-" + entity;
        KafkaSender sender = (topic, key, value, callback) -> {
            throw new AssertionError("kafka not exercised");
        };

        engine = new CdcEngine(
                "test",
                Arrays.asList(mapping1, mapping2),
                src,
                new SnapshotStore(),
                NoopSnapshotPersistence.INSTANCE,
                new EngineConfig(1, 500_000, 5, 5, 10_000, 2),
                topicResolver,
                new SyncEventPublisher(sender),
                tracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                k -> null);

        engine.start();

        long deadline = System.currentTimeMillis() + 5_000L;
        Set<String> ticked = new HashSet<>();
        while (System.currentTimeMillis() < deadline && ticked.size() < 2) {
            for (app.l2nx.gs.adapter.api.kafka.ops.EntityStats s : tracker.currentStatuses()) {
                ticked.add(s.getName());
            }
            if (ticked.size() < 2) {
                Thread.sleep(50);
            }
        }
        assertTrue(ticked.contains("clan"), "clan entity must tick on shared pool");
        assertTrue(ticked.contains("alpha"), "alpha entity must tick on shared pool — no starvation");
    }

    private static EntityMapping<?> renameEntity(EntityMapping<?> base, String newName) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        EntityMapping erased = base;
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return newName;
            }

            @Override
            public Class<Object> dtoType() {
                return Object.class;
            }

            @Override
            public app.l2nx.gs.adapter.api.spi.PrimarySource<?> primary() {
                return erased.primary();
            }

            @Override
            @SuppressWarnings("unchecked")
            public List<app.l2nx.gs.adapter.api.spi.ChildSource<?>> children() {
                return erased.children();
            }

            @Override
            public Object mapEntity(Object primaryRow, java.util.Map<String, List<Object>> childRowsByTable) {
                return primaryRow;
            }
        };
    }
}
