package app.l2nx.gs.db.sync.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.persist.SnapshotPersistence;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Engine-level routing of the targeted force-resync fast-path: a triggered
 * per-PK resync must issue an {@code IN}-list Phase-1 query (hash only the
 * targeted rows), while a scheduled tick and a whole-entity resync must full-
 * scan ({@code BETWEEN}). The empty host DB makes invalidated snapshot entries
 * diff to DELETE, so the published op/PK doubles as a behavioral check.
 *
 * <p>The connection is a deep-stub that records every prepared Phase-1 SQL,
 * mirroring {@code CdcEngineForceResyncTest} but capturing the query shape.</p>
 */
class CdcEngineTargetedResyncTest {

    private static final RecordMetadata META = new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);

    private final SnapshotStore snapshot = new SnapshotStore();
    private final RecordingSource source = new RecordingSource();
    private final List<Object> published = new CopyOnWriteArrayList<Object>();
    private final EntityStatsTracker statsTracker = new EntityStatsTracker();

    private CdcEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Nested
    class TriggeredPerPk {

        @Test
        void runGuardedCycle_shouldHashOnlyTargetedRows_whenTriggeredWithBoundedPkSet() {
            snapshot.putCrc("clan", 1L, 100);
            snapshot.putCrc("clan", 2L, 200);
            snapshot.putCrc("clan", 3L, 300);
            engine = buildEngine();
            engine.start();
            source.clear();

            engine.requestPkRepublishNoEvent("clan", pks(2L));

            await(() -> source.primaryInSql() != null);
            // Targeted Phase-1: IN-list carrying only the targeted PK, never a range scan.
            assertTrue(source.primaryInSql().contains("clan_id IN (?)"));
            assertFalse(source.anyPrimaryBetween(), "a triggered per-PK resync must NOT range-scan");
            // Empty host DB → only the targeted, invalidated PK 2 diffs to DELETE.
            await(() -> published.size() == 1);
            assertEquals("DELETED", opOf(published.get(0)));
            assertEquals(2L, pkOf(published.get(0)));
        }
    }

    @Nested
    class ScheduledTick {

        @Test
        void runGuardedCycle_shouldFullScan_whenScheduledTick() throws Exception {
            snapshot.putCrc("clan", 5L, 500);
            engine = buildEngine();
            engine.start();
            source.clear();

            // tickOnceSynchronously runs the scheduled (full-scan) path.
            for (java.util.concurrent.Future<?> f : engine.tickOnceSynchronously()) {
                f.get();
            }

            assertTrue(source.anyPrimaryBetween(), "a scheduled tick must full-scan via BETWEEN");
            assertNull(source.primaryInSql(), "a scheduled tick must never use an IN-list");
        }

        /**
         * The central spec invariant end-to-end: a SCHEDULED tick must NEVER
         * take the targeted fast-path, even while a per-PK resync is pending.
         * Drives the REAL {@code runGuardedCycle(entity, slot, triggered=false)}
         * routing (not {@code task.runCycle()} directly) with a pending no-event
         * PK set staged WITHOUT an immediate trigger, and asserts the cycle
         * still full-scans (BETWEEN, not IN) — i.e. the scheduled tick drains
         * the pending set into the snapshot but ignores it for query narrowing.
         */
        @Test
        void scheduledTick_shouldFullScan_evenWhenPerPkResyncPending() throws Exception {
            snapshot.putCrc("clan", 7L, 700);
            engine = buildEngine();
            engine.start();
            source.clear();

            // Stage a pending per-PK resync but do NOT trigger an immediate cycle,
            // so the targeted set is still pending when the scheduled tick runs.
            engine.enqueueNoEventPksWithoutTrigger("clan", pks(7L));

            engine.runScheduledTickNow("clan").get();

            assertTrue(
                    source.anyPrimaryBetween(),
                    "a scheduled tick must full-scan via BETWEEN even with a per-PK resync pending");
            assertNull(
                    source.primaryInSql(),
                    "a scheduled tick must never narrow to an IN-list from the pending targeted set");
        }
    }

    @Nested
    class WholeEntityResync {

        @Test
        void runGuardedCycle_shouldFullScan_whenWholeEntityResyncPending() {
            snapshot.putCrc("clan", 9L, 900);
            engine = buildEngine();
            engine.start();
            source.clear();

            UUID resyncId = UUID.fromString("018f0000-0000-7000-8000-0000000000ff");
            engine.requestForceResync(resyncId, "clan");

            await(() -> source.anyPrimaryBetween());
            assertNull(source.primaryInSql(), "a whole-entity resync forces the full scan even though it is triggered");
        }
    }

    private static String opOf(Object e) {
        return ((app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent<?>) e).getOp();
    }

    private static long pkOf(Object e) {
        return ((app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent<?>) e).getPk();
    }

    private CdcEngine buildEngine() {
        KafkaSender sender = (topic, key, value, callback) -> {
            published.add(value);
            callback.onCompletion(META, null);
        };
        SnapshotPersistence persistence = new SnapshotPersistence() {
            @Override
            public void load(SnapshotStore target) {}

            @Override
            public void checkpoint(String entityName, SnapshotStore src) {}

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

    /**
     * Deep-stub JDBC source that records every Phase-1 primary SQL string a
     * cycle prepares (the {@code clan_data} hash query) and answers an empty
     * result set so invalidated snapshot rows diff to DELETE.
     */
    private static final class RecordingSource implements JdbcConnectionSource {
        private final List<String> primarySql = new CopyOnWriteArrayList<String>();

        void clear() {
            primarySql.clear();
        }

        String primaryInSql() {
            for (String sql : primarySql) {
                if (sql.contains("clan_id IN (")) {
                    return sql;
                }
            }
            return null;
        }

        boolean anyPrimaryBetween() {
            for (String sql : primarySql) {
                if (sql.contains("clan_id BETWEEN")) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String name() {
            return "recording";
        }

        @Override
        public Connection getConnection() throws java.sql.SQLException {
            Connection conn = mock(Connection.class, RETURNS_DEEP_STUBS);
            when(conn.getAutoCommit()).thenReturn(true);
            when(conn.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
                    .thenAnswer(inv -> {
                        String sql = inv.getArgument(0);
                        // Only the primary hash query carries clan_id without BIT_XOR; both
                        // primary and child hash queries route here, record any clan_id one.
                        if (sql.contains("clan_id")) {
                            primarySql.add(sql);
                        }
                        PreparedStatement ps = mock(PreparedStatement.class, RETURNS_DEEP_STUBS);
                        ResultSet rs = mock(ResultSet.class);
                        when(rs.next()).thenReturn(false);
                        when(ps.executeQuery()).thenReturn(rs);
                        return ps;
                    });
            return conn;
        }

        @Override
        public Optional<PoolStats> stats() {
            return Optional.empty();
        }
    }
}
