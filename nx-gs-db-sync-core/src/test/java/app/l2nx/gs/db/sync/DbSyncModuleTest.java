package app.l2nx.gs.db.sync;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanDbDto;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.DbSchemaProvider;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.TestMappings;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class DbSyncModuleTest {

    private static final Map<String, String> CLAN_TOPIC = Collections.singletonMap("clan", "bohpts.gs.sync.clans");

    private static final ConnectContext CTX_WITH_TOPICS = ctx(CLAN_TOPIC);
    private static final ConnectContext CTX_NO_TOPICS = ctx(null);

    private static final KafkaSender NEVER_CALLED = (topic, key, value, callback) -> {
        throw new AssertionError("KafkaSender must not be called from constructor-only paths");
    };

    @Test
    void name_shouldBe_dbSync() {
        assertEquals("db-sync", new DbSyncModule().name());
    }

    @Test
    void currentStatus_beforeOnConnect_shouldReportInitWithEmptyStats() {
        DbSyncModule module = build(emptyJdbc(), emptySchema(), passSmoke());

        ModuleStatus status = module.currentStatus();

        assertEquals("INIT", status.getState());
        assertSame(ModuleStatus.Stats.empty(), status.getStats());
    }

    @Test
    void onConnect_shouldDisable_whenSyncTopicsEmpty() {
        DbSyncModule module = build(singleJdbc(stub("a", null)), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_NO_TOPICS);

        assertEquals("DISABLED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldFail_whenNoJdbcSource() {
        DbSyncModule module = build(emptyJdbc(), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertEquals("FAILED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldFail_whenMultipleJdbcSources() {
        DbSyncModule module =
                build(() -> Arrays.asList(stub("a", null), stub("b", null)), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertEquals("FAILED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldDisable_whenNoSchemaProvider() {
        DbSyncModule module = build(singleJdbc(stub("a", null)), emptySchema(), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertEquals("DISABLED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldFail_whenMultipleSchemaProviders() {
        DbSyncModule module =
                build(singleJdbc(stub("a", null)), () -> Arrays.asList(clanProvider(), clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertEquals("FAILED", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldBecomeActive_whenAllResolvedAndSmokePasses() {
        DbSyncModule module = build(singleJdbc(stub("a", null)), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertEquals("ACTIVE", module.currentStatus().getState());
    }

    @Test
    void onConnect_shouldBecomeDegraded_whenSmokeCheckFails() {
        PoolStats pool = new PoolStats(0, 4, 4, null);
        DbSyncModule module = build(singleJdbc(stub("a", pool)), singleSchema(clanProvider()), failSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        ModuleStatus status = module.currentStatus();
        assertEquals("DEGRADED", status.getState());
        assertEquals(Optional.of(pool), status.getStats().getPool());
    }

    @Test
    void currentStatus_shouldSurfacePoolStats_whenSourceProvidesThem() {
        PoolStats pool = new PoolStats(2, 6, 8, null);
        DbSyncModule module = build(singleJdbc(stub("a", pool)), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertEquals(Optional.of(pool), module.currentStatus().getStats().getPool());
    }

    @Test
    void currentStatus_shouldNotPropagate_whenSrcStatsThrows() {
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
        DbSyncModule module = build(singleJdbc(src), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);

        assertFalse(module.currentStatus().getStats().getPool().isPresent());
    }

    @Test
    void onDisconnect_shouldClearSourceReference() {
        PoolStats pool = new PoolStats(1, 1, 2, null);
        DbSyncModule module = build(singleJdbc(stub("a", pool)), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_WITH_TOPICS);
        assertTrue(module.currentStatus().getStats().getPool().isPresent());

        module.onDisconnect();

        assertFalse(module.currentStatus().getStats().getPool().isPresent());
    }

    @Test
    void smokeChecker_shouldReceiveResolvedSource() {
        JdbcConnectionSource src = stub("seen", null);
        java.util.concurrent.atomic.AtomicReference<JdbcConnectionSource> seen =
                new java.util.concurrent.atomic.AtomicReference<JdbcConnectionSource>();
        Predicate<JdbcConnectionSource> spy = s -> {
            seen.set(s);
            return true;
        };
        DbSyncModule module = build(singleJdbc(src), singleSchema(clanProvider()), spy);

        module.onConnect(CTX_WITH_TOPICS);

        assertSame(src, seen.get());
    }

    @Test
    void stop_shouldBeNoOp_whenEngineNeverStarted() {
        DbSyncModule module = build(emptyJdbc(), emptySchema(), passSmoke());

        module.stop(); // no engine yet
        module.onDisconnect();

        assertEquals("INIT", module.currentStatus().getState());
    }

    @Test
    void start_shouldBeNoOp_whenStateDisabled() {
        DbSyncModule module = build(singleJdbc(stub("a", null)), singleSchema(clanProvider()), passSmoke());

        module.onConnect(CTX_NO_TOPICS); // → DISABLED
        module.start();

        // Engine never started — stays DISABLED, currentStatus carries no entities.
        ModuleStatus status = module.currentStatus();
        assertEquals("DISABLED", status.getState());
        assertFalse(status.getStats().getEntities().isPresent());
    }

    private static DbSyncModule build(
            Supplier<List<JdbcConnectionSource>> jdbc,
            Supplier<List<DbSchemaProvider>> schema,
            Predicate<JdbcConnectionSource> smoke) {
        Function<String, String> noSysprops = k -> null;
        return new DbSyncModule(jdbc, schema, smoke, noSysprops, NEVER_CALLED);
    }

    private static ConnectContext ctx(Map<String, String> dbTopics) {
        app.l2nx.gs.adapter.api.rest.SyncTopics topics = dbTopics == null
                ? null
                : app.l2nx.gs.adapter.api.rest.SyncTopics.builder().db(dbTopics).build();
        return ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme Primary")
                .adapterVersion("0.1.0")
                .syncTopics(topics)
                .build();
    }

    private static Supplier<List<JdbcConnectionSource>> emptyJdbc() {
        return Collections::emptyList;
    }

    private static Supplier<List<JdbcConnectionSource>> singleJdbc(JdbcConnectionSource src) {
        return () -> Collections.singletonList(src);
    }

    private static Supplier<List<DbSchemaProvider>> emptySchema() {
        return Collections::emptyList;
    }

    private static Supplier<List<DbSchemaProvider>> singleSchema(DbSchemaProvider p) {
        return () -> Collections.singletonList(p);
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

    private static DbSchemaProvider clanProvider() {
        return new DbSchemaProvider() {
            @Override
            public String schemaName() {
                return "test";
            }

            @Override
            public List<EntityMapping<?>> mappings() {
                return Collections.singletonList(clanMapping());
            }
        };
    }

    private static EntityMapping<ClanDbDto> clanMapping() {
        return TestMappings.clanOnly();
    }

    @SuppressWarnings("unused")
    private static void unusedNotNull() {
        assertNotNull(new Object());
    }
}
