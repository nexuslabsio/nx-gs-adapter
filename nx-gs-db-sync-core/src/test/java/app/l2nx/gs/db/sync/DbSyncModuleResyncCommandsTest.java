package app.l2nx.gs.db.sync;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.sync.ResyncEntitiesCommand;
import app.l2nx.gs.adapter.api.kafka.commands.sync.ResyncEntitiesResult;
import app.l2nx.gs.adapter.api.kafka.commands.sync.ResyncRowsCommand;
import app.l2nx.gs.adapter.api.kafka.commands.sync.ResyncRowsResult;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.*;
import app.l2nx.gs.db.sync.engine.EngineConfig;
import app.l2nx.gs.db.sync.engine.TestMappings;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import com.google.gson.Gson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbSyncModuleResyncCommandsTest {

    private static final UUID RESYNC_ID = UUID.fromString("018f0000-0000-7000-8000-0000000000dd");
    private static final Gson GSON = new Gson();

    private static final KafkaSender DROPPING_SENDER = (topic, key, value, callback) ->
            callback.onCompletion(null, new RuntimeException("test sender drops everything"));

    @TempDir
    Path tempDir;

    private final RecordingCommands commands = new RecordingCommands();
    private final CommandContext cctx = mock(CommandContext.class);
    private DbSyncModule module;

    @AfterEach
    void tearDown() {
        if (module != null) {
            module.stop();
        }
    }

    @Test
    void onConnect_shouldRegisterBothResyncHandlers() {
        module = build(failingJdbc());

        module.onConnect(ctx());

        assertTrue(commands.handlers.containsKey(ResyncEntitiesCommand.class));
        assertTrue(commands.handlers.containsKey(ResyncRowsCommand.class));
    }

    @Test
    void handleResyncEntities_shouldReturnUnavailable_whenEngineNotRunning() {
        module = build(failingJdbc());
        module.onConnect(ctx());
        // start() not called — engine is null.

        CommandResult<ResyncEntitiesResult> result = module.handleResyncEntities(
                ResyncEntitiesCommand.builder().resyncId(RESYNC_ID).build(), cctx);

        assertEquals(CommandStatus.UNAVAILABLE, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldReturnUnavailable_whenEngineNotRunning() {
        module = build(failingJdbc());
        module.onConnect(ctx());

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                rowsCommand("character", 1L), cctx);

        assertEquals(CommandStatus.UNAVAILABLE, result.getStatus());
    }

    @Test
    void handleResyncEntities_shouldAcceptAllMappedEntities_whenEntitiesOmitted() {
        module = startedModule(failingJdbc());

        CommandResult<ResyncEntitiesResult> result = module.handleResyncEntities(
                ResyncEntitiesCommand.builder().resyncId(RESYNC_ID).build(), cctx);

        assertTrue(result.isOk());
        assertEquals(Arrays.asList("character", "item"), result.getPayload().getAcceptedEntities());
    }

    @Test
    void handleResyncEntities_shouldAcceptRequestedSubset() {
        module = startedModule(failingJdbc());

        CommandResult<ResyncEntitiesResult> result = module.handleResyncEntities(
                ResyncEntitiesCommand.builder()
                        .resyncId(RESYNC_ID)
                        .entities(Collections.singletonList("item"))
                        .build(),
                cctx);

        assertTrue(result.isOk());
        assertEquals(Collections.singletonList("item"), result.getPayload().getAcceptedEntities());
    }

    @Test
    void handleResyncEntities_shouldReturnValidationFailed_whenAnyEntityUnknown() {
        module = startedModule(failingJdbc());

        CommandResult<ResyncEntitiesResult> result = module.handleResyncEntities(
                ResyncEntitiesCommand.builder()
                        .resyncId(RESYNC_ID)
                        .entities(Arrays.asList("character", "nope"))
                        .build(),
                cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @Test
    void handleResyncEntities_shouldReturnValidationFailed_whenResyncIdMissingOnWire() {
        module = startedModule(failingJdbc());
        ResyncEntitiesCommand wire = GSON.fromJson("{\"entities\":[\"character\"]}",
                ResyncEntitiesCommand.class);

        CommandResult<ResyncEntitiesResult> result = module.handleResyncEntities(wire, cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldReturnValidationFailed_whenEntityUnknown() {
        module = startedModule(failingJdbc());

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                rowsCommand("nope", 1L), cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldReturnValidationFailed_whenPksEmptyOnWire() {
        module = startedModule(failingJdbc());
        ResyncRowsCommand wire = GSON.fromJson(
                "{\"resyncId\":\"" + RESYNC_ID + "\",\"entityName\":\"character\",\"pks\":[]}",
                ResyncRowsCommand.class);

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(wire, cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldReturnValidationFailed_whenPksOverCapOnWire() {
        module = startedModule(failingJdbc());
        StringBuilder pks = new StringBuilder();
        for (int i = 0; i <= ResyncRowsCommand.MAX_PKS; i++) {
            if (i > 0) pks.append(',');
            pks.append(i);
        }
        ResyncRowsCommand wire = GSON.fromJson(
                "{\"resyncId\":\"" + RESYNC_ID + "\",\"entityName\":\"character\",\"pks\":["
                        + pks + "]}",
                ResyncRowsCommand.class);

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(wire, cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
    void handleResyncRows_shouldReturnValidationFailed_whenPkNonPositive(long pk) {
        module = startedModule(failingJdbc());

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                rowsCommand("character", 1L, pk), cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldReturnValidationFailed_whenPksContainNullOnWire() {
        module = startedModule(failingJdbc());
        ResyncRowsCommand wire = GSON.fromJson(
                "{\"resyncId\":\"" + RESYNC_ID + "\",\"entityName\":\"character\",\"pks\":[1,null]}",
                ResyncRowsCommand.class);

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(wire, cctx);

        assertEquals(CommandStatus.VALIDATION_FAILED, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldReportTargetCountOnly_whenCascadeOff() {
        module = startedModule(failingJdbc());

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                rowsCommand("character", 1L, 2L, 2L), cctx);

        assertTrue(result.isOk());
        Map<String, Integer> counts = result.getPayload().getInvalidatedByEntity();
        assertEquals(Collections.singleton("character"), counts.keySet());
        assertEquals(Integer.valueOf(2), counts.get("character"), "duplicate PKs deduplicate");
    }

    @Test
    void handleResyncRows_shouldResolveCascadeChildren_andReportPerEntityCounts() throws SQLException {
        module = startedModule(cascadeJdbc(100L, 101L, 102L));

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                ResyncRowsCommand.builder()
                        .resyncId(RESYNC_ID)
                        .entityName("character")
                        .pks(Arrays.asList(1L, 2L))
                        .cascade(true)
                        .build(),
                cctx);

        assertTrue(result.isOk());
        Map<String, Integer> counts = result.getPayload().getInvalidatedByEntity();
        assertEquals(Integer.valueOf(2), counts.get("character"));
        assertEquals(Integer.valueOf(3), counts.get("item"));
    }

    @Test
    void handleResyncRows_shouldReturnInternalError_whenCascadeResolutionFails() throws SQLException {
        JdbcConnectionSource src = jdbc(() -> {
            throw new SQLException("db down");
        });
        module = startedModule(src);

        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                ResyncRowsCommand.builder()
                        .resyncId(RESYNC_ID)
                        .entityName("character")
                        .pks(Collections.singletonList(1L))
                        .cascade(true)
                        .build(),
                cctx);

        assertEquals(CommandStatus.INTERNAL_ERROR, result.getStatus());
    }

    @Test
    void handleResyncRows_shouldOmitCascade_whenTargetEntityHasNoChildren() {
        module = startedModule(failingJdbc());

        // "item" declares character as parent; nothing declares item as parent.
        CommandResult<ResyncRowsResult> result = module.handleResyncRows(
                ResyncRowsCommand.builder()
                        .resyncId(RESYNC_ID)
                        .entityName("item")
                        .pks(Collections.singletonList(9L))
                        .cascade(true)
                        .build(),
                cctx);

        assertTrue(result.isOk());
        assertEquals(Collections.singleton("item"),
                result.getPayload().getInvalidatedByEntity().keySet());
    }

    @Test
    void start_shouldFail_whenParentRefReferencesUnknownEntity() {
        DbSchemaProvider provider = provider(
                stub("character"),
                withParentRefs(stub("item"),
                        Collections.singletonList(ParentRef.of("ghost", "owner_id"))));
        module = build(failingJdbc(), provider);
        module.onConnect(ctx());

        module.start();

        assertEquals("FAILED", module.currentStatus().getState());
    }

    @Test
    void start_shouldFail_whenParentRefFkColumnInvalid() {
        DbSchemaProvider provider = provider(
                stub("character"),
                withParentRefs(stub("item"),
                        Collections.singletonList(ParentRef.of("character", "owner_id; DROP TABLE x"))));
        module = build(failingJdbc(), provider);
        module.onConnect(ctx());

        module.start();

        assertEquals("FAILED", module.currentStatus().getState());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Fixtures
    // ─────────────────────────────────────────────────────────────────────

    private DbSyncModule startedModule(JdbcConnectionSource src) {
        DbSyncModule built = build(src);
        built.onConnect(ctx());
        built.start();
        assertEquals("ACTIVE", built.currentStatus().getState());
        return built;
    }

    private DbSyncModule build(JdbcConnectionSource src) {
        return build(src, provider(
                stub("character"),
                withParentRefs(stub("item"),
                        Collections.singletonList(ParentRef.of("character", "owner_id")))));
    }

    private DbSyncModule build(JdbcConnectionSource src, DbSchemaProvider provider) {
        Map<String, String> config = new HashMap<String, String>();
        config.put(EngineConfig.KEY_TICK_INTERVAL_SECONDS, "3600");
        config.put(EngineConfig.KEY_PERSIST_DIR, tempDir.toString());
        Function<String, String> configSource = config::get;
        return new DbSyncModule(
                () -> Collections.singletonList(src),
                () -> Collections.singletonList(provider),
                s -> true,
                configSource,
                DROPPING_SENDER);
    }

    private ConnectContext ctx() {
        Map<String, String> dbTopics = new LinkedHashMap<String, String>();
        dbTopics.put("character", "test.gs.sync.characters");
        dbTopics.put("item", "test.gs.sync.items");
        return ConnectContext.builder()
                .tenantId(UUID.randomUUID()).tenantSlug("acme")
                .serverId(UUID.randomUUID()).serverSlug("primary").serverName("Acme Primary")
                .adapterVersion("0.1.0")
                .syncTopics(SyncTopics.builder().db(dbTopics).build())
                .commands(commands)
                .build();
    }

    private static ResyncRowsCommand rowsCommand(String entityName, Long... pks) {
        return ResyncRowsCommand.builder()
                .resyncId(RESYNC_ID)
                .entityName(entityName)
                .pks(Arrays.asList(pks))
                .build();
    }

    private static DbSchemaProvider provider(EntityMapping<?>... mappings) {
        List<EntityMapping<?>> list = Arrays.asList(mappings);
        return new DbSchemaProvider() {
            @Override
            public String schemaName() {
                return "test";
            }

            @Override
            public List<EntityMapping<?>> mappings() {
                return list;
            }
        };
    }

    private static EntityMapping<Object> stub(String entityName) {
        return TestMappings.stub(entityName, entityName + "s", "obj_id",
                Collections.singletonList("name"));
    }

    private static EntityMapping<Object> withParentRefs(EntityMapping<Object> delegate,
                                                        List<ParentRef> refs) {
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return delegate.entityName();
            }

            @Override
            public Class<Object> dtoType() {
                return delegate.dtoType();
            }

            @Override
            public PrimarySource<?> primary() {
                return delegate.primary();
            }

            @Override
            public List<ChildSource<?>> children() {
                return delegate.children();
            }

            @Override
            public List<ParentRef> parentRefs() {
                return refs;
            }

            @Override
            public Object mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                return delegate.mapEntity(primaryRow, childRowsByTable);
            }
        };
    }

    private static JdbcConnectionSource failingJdbc() {
        return jdbc(() -> {
            throw new SQLException("no real db in this test");
        });
    }

    private static JdbcConnectionSource cascadeJdbc(long... childPks) throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        Boolean[] nextTail = new Boolean[childPks.length];
        for (int i = 0; i < childPks.length; i++) {
            nextTail[i] = i < childPks.length - 1;
        }
        when(rs.next()).thenReturn(true, nextTail);
        Long first = childPks[0];
        Long[] rest = new Long[childPks.length - 1];
        for (int i = 1; i < childPks.length; i++) {
            rest[i - 1] = childPks[i];
        }
        when(rs.getLong(1)).thenReturn(first, rest);
        return jdbc(() -> conn);
    }

    private static JdbcConnectionSource jdbc(ConnectionSupplier supplier) {
        return new JdbcConnectionSource() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public Connection getConnection() throws SQLException {
                return supplier.get();
            }

            @Override
            public Optional<PoolStats> stats() {
                return Optional.empty();
            }
        };
    }

    @FunctionalInterface
    private interface ConnectionSupplier {
        Connection get() throws SQLException;
    }

    private static final class RecordingCommands implements NxCommands {
        final Map<Class<?>, CommandHandler<?, ?>> handlers =
                new HashMap<Class<?>, CommandHandler<?, ?>>();

        @Override
        public <R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler) {
            handlers.put(type, handler);
        }
    }
}
