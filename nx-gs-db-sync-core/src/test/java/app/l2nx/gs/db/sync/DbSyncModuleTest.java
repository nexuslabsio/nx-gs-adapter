package app.l2nx.gs.db.sync;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbSyncModuleTest {

    private final ConnectContext ctx = ConnectContext.builder()
            .tenantId(UUID.randomUUID()).tenantSlug("acme")
            .serverId(UUID.randomUUID()).serverSlug("primary").serverName("Acme Primary")
            .adapterVersion("0.1.0")
            .build();

    @Test
    void name_shouldBe_dbSync() {
        assertEquals("db-sync", new DbSyncModule().name());
    }

    @Test
    void currentStatus_beforeOnConnect_shouldReportInitWithEmptyStats() {
        DbSyncModule module = new DbSyncModule(emptyDiscoverer(), passSmoke());

        ModuleStatus status = module.currentStatus();

        assertEquals("INIT", status.getState());
        assertSame(ModuleStatus.Stats.empty(), status.getStats());
    }

    @Test
    void onConnect_shouldFail_whenNoSourceFound() {
        DbSyncModule module = new DbSyncModule(emptyDiscoverer(), passSmoke());

        module.onConnect(ctx);

        assertEquals("FAILED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldFail_whenMultipleSourcesFound() {
        JdbcConnectionSource a = stub("a", null);
        JdbcConnectionSource b = stub("b", null);
        DbSyncModule module = new DbSyncModule(() -> Arrays.asList(a, b), passSmoke());

        module.onConnect(ctx);

        assertEquals("FAILED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldBecomeActive_whenSmokeCheckPasses() {
        JdbcConnectionSource src = stub("stub", null);
        DbSyncModule module = new DbSyncModule(singleton(src), passSmoke());

        module.onConnect(ctx);

        assertEquals("ACTIVE", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldBecomeDegraded_whenSmokeCheckFails() {
        JdbcConnectionSource src = stub("stub", new PoolStats(0, 4, 4));
        DbSyncModule module = new DbSyncModule(singleton(src), failSmoke());

        module.onConnect(ctx);

        ModuleStatus status = module.currentStatus();
        assertEquals("DEGRADED", status.getState());
        // Source ref retained so pool stats still bubble up while smoke is degraded.
        assertEquals(Optional.of(new PoolStats(0, 4, 4)), status.getStats().getPool());
    }

    @Test
    void currentStatus_shouldSurfacePoolStats_whenSourceProvidesThem() {
        PoolStats pool = new PoolStats(2, 6, 8);
        JdbcConnectionSource src = stub("stub", pool);
        DbSyncModule module = new DbSyncModule(singleton(src), passSmoke());

        module.onConnect(ctx);

        assertEquals(Optional.of(pool), module.currentStatus().getStats().getPool());
    }

    @Test
    void currentStatus_shouldDegradeStatsToEmpty_whenSrcStatsThrows() {
        JdbcConnectionSource src = new JdbcConnectionSource() {
            @Override
            public String name() {
                return "throwing";
            }

            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("not used");
            }

            @Override
            public Optional<PoolStats> stats() {
                throw new RuntimeException("buggy spi");
            }
        };
        DbSyncModule module = new DbSyncModule(singleton(src), passSmoke());

        module.onConnect(ctx);

        assertFalse(module.currentStatus().getStats().getPool().isPresent());
    }

    @Test
    void onDisconnect_shouldClearSourceReference() {
        JdbcConnectionSource src = stub("stub", new PoolStats(1, 1, 2));
        DbSyncModule module = new DbSyncModule(singleton(src), passSmoke());

        module.onConnect(ctx);
        assertTrue(module.currentStatus().getStats().getPool().isPresent());

        module.onDisconnect();

        assertFalse(module.currentStatus().getStats().getPool().isPresent());
    }

    @Test
    void smokeChecker_shouldReceiveResolvedSource() {
        JdbcConnectionSource src = stub("seen", null);
        AtomicReference<JdbcConnectionSource> seen = new AtomicReference<JdbcConnectionSource>();
        Predicate<JdbcConnectionSource> spy = s -> {
            seen.set(s);
            return true;
        };
        DbSyncModule module = new DbSyncModule(singleton(src), spy);

        module.onConnect(ctx);

        assertSame(src, seen.get());
    }

    @Test
    void start_stop_onDisconnect_shouldBeNoOpsInPhase1() {
        DbSyncModule module = new DbSyncModule(emptyDiscoverer(), passSmoke());

        // None of these throw; nothing observable changes besides onDisconnect clearing source.
        module.start();
        module.stop();
        module.onDisconnect();

        assertEquals("INIT", module.currentStatus().getState());
    }

    private static Supplier<List<JdbcConnectionSource>> emptyDiscoverer() {
        return Collections::emptyList;
    }

    private static Supplier<List<JdbcConnectionSource>> singleton(JdbcConnectionSource src) {
        return () -> Collections.singletonList(src);
    }

    private static Predicate<JdbcConnectionSource> passSmoke() {
        return s -> true;
    }

    private static Predicate<JdbcConnectionSource> failSmoke() {
        return s -> false;
    }

    private static JdbcConnectionSource stub(String name, PoolStats pool) {
        return new JdbcConnectionSource() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("not used in this test path");
            }

            @Override
            public Optional<PoolStats> stats() {
                return pool != null ? Optional.of(pool) : Optional.<PoolStats>empty();
            }
        };
    }
}
