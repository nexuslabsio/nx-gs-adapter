package app.l2nx.gs.db.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import app.l2nx.gs.adapter.api.kafka.events.sync.ResyncCompletedEvent;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.DbSchemaProvider;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.adapter.api.spi.NxEvents;
import app.l2nx.gs.adapter.api.spi.NxSync;
import app.l2nx.gs.adapter.api.spi.NxSyncResyncHandler;
import app.l2nx.gs.adapter.api.spi.NxSyncTrigger;
import app.l2nx.gs.adapter.api.spi.ParentRef;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.EngineConfig;
import app.l2nx.gs.db.sync.engine.TestMappings;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end wiring of {@code NxSync.requestResync} through {@link DbSyncModule}:
 * the registered {@link NxSyncResyncHandler} resolves cascade children via
 * {@link EntityMapping#parentRefs()} and force-republishes the parent + each
 * cascaded child on the engine's no-event path — emitting NO
 * {@link ResyncCompletedEvent}.
 */
class DbSyncModuleNxSyncResyncTest {

    private static final RecordMetadata META = new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);

    @TempDir
    Path tempDir;

    private final RecordingSync sync = new RecordingSync();
    private final List<Object> events = Collections.synchronizedList(new ArrayList<Object>());
    private final List<String> publishedTopics = Collections.synchronizedList(new ArrayList<String>());
    private DbSyncModule module;

    @AfterEach
    void tearDown() {
        if (module != null) {
            module.stop();
        }
    }

    @Test
    void requestResync_shouldForceRepublishCharacterAndItems_withoutCompletionEvent_whenCascade() throws Exception {
        module = startedModule(cascadeJdbc(100L, 101L));

        // Caller-side fire-and-forget: route a per-command resync for character 1.
        sync.requestResync("character", Collections.singletonList(1L), true);

        // The forced cycle borrows the (empty) DB and diffs the invalidated
        // sentinels to DELETED publishes on both topics.
        awaitTopic("test.gs.sync.characters");
        awaitTopic("test.gs.sync.items");

        // No tracked admin resync → no completion event ever published.
        for (Object e : events) {
            assertTrue(
                    !(e instanceof ResyncCompletedEvent),
                    "no-event per-command resync must not emit ResyncCompletedEvent");
        }
    }

    private void awaitTopic(String topic) {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (publishedTopics.contains(topic)) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("no publish on topic " + topic + " within 10s; saw " + publishedTopics);
    }

    private DbSyncModule startedModule(JdbcConnectionSource src) {
        DbSchemaProvider provider = provider(
                stub("character"),
                withParentRefs(stub("item"), Collections.singletonList(ParentRef.of("character", "owner_id"))));
        Map<String, String> config = new HashMap<String, String>();
        config.put(EngineConfig.KEY_TICK_INTERVAL_SECONDS, "3600");
        config.put(EngineConfig.KEY_PERSIST_DIR, tempDir.toString());
        Function<String, String> configSource = config::get;
        KafkaSender capturingSender = (topic, key, value, callback) -> {
            publishedTopics.add(topic);
            callback.onCompletion(META, null);
        };
        DbSyncModule built = new DbSyncModule(
                () -> Collections.singletonList(src),
                () -> Collections.singletonList(provider),
                s -> true,
                configSource,
                capturingSender);
        built.onConnect(ctx());
        built.start();
        assertEquals("ACTIVE", built.currentStatus().getState());
        return built;
    }

    private ConnectContext ctx() {
        Map<String, String> dbTopics = new LinkedHashMap<String, String>();
        dbTopics.put("character", "test.gs.sync.characters");
        dbTopics.put("item", "test.gs.sync.items");
        return ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme Primary")
                .adapterVersion("0.1.0")
                .syncTopics(SyncTopics.builder().db(dbTopics).build())
                .events(new NxEvents() {
                    @Override
                    public void publish(Object event) {
                        events.add(event);
                    }

                    @Override
                    public boolean flush(long timeoutMs) {
                        return true;
                    }
                })
                .io(Runnable::run)
                .sync(sync)
                .build();
    }

    private static EntityMapping<Object> stub(String entityName) {
        return TestMappings.stub(entityName, entityName + "s", "obj_id", Collections.singletonList("name"));
    }

    private static EntityMapping<Object> withParentRefs(EntityMapping<Object> delegate, List<ParentRef> refs) {
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

    /**
     * Connection returning child PKs only for the cascade {@code SELECT obj_id
     * FROM items WHERE owner_id IN (...)}; every other statement the cycle runs
     * (window MIN/MAX, page scans) gets a deep-stubbed empty result, so the
     * snapshot's invalidation sentinels diff to DELETED against an empty DB.
     */
    private JdbcConnectionSource cascadeJdbc(long... childPks) {
        return new JdbcConnectionSource() {
            @Override
            public String name() {
                return "cascade";
            }

            @Override
            public Connection getConnection() throws SQLException {
                Connection conn = mock(Connection.class, RETURNS_DEEP_STUBS);
                PreparedStatement cascade = mock(PreparedStatement.class);
                ResultSet rs = mock(ResultSet.class);
                Boolean[] nextTail = new Boolean[childPks.length];
                for (int i = 0; i < childPks.length; i++) {
                    nextTail[i] = i < childPks.length - 1;
                }
                when(rs.next()).thenReturn(childPks.length > 0, nextTail);
                Long first = childPks.length > 0 ? childPks[0] : 0L;
                Long[] rest = new Long[Math.max(0, childPks.length - 1)];
                for (int i = 1; i < childPks.length; i++) {
                    rest[i - 1] = childPks[i];
                }
                when(rs.getLong(1)).thenReturn(first, rest);
                when(cascade.executeQuery()).thenReturn(rs);
                when(conn.prepareStatement(anyString())).thenAnswer(inv -> {
                    String sql = inv.getArgument(0);
                    return sql.contains("owner_id IN") ? cascade : mock(PreparedStatement.class, RETURNS_DEEP_STUBS);
                });
                return conn;
            }

            @Override
            public Optional<PoolStats> stats() {
                return Optional.empty();
            }
        };
    }

    /**
     * Real {@link NxSync} routing under test for {@code requestResync}: invokes
     * the registered {@link NxSyncResyncHandler} synchronously (handler hops its
     * own IO via {@code ctx.io()}, wired to direct-run here). Trigger
     * registration is captured but unused.
     */
    private static final class RecordingSync implements NxSync {
        private volatile NxSyncResyncHandler handler;

        @Override
        public void requestNow(String entityName, long pk) {}

        @Override
        public void requestNow(String entityName, Collection<Long> pks) {}

        @Override
        public void requestResync(String entityName, Collection<Long> pks, boolean cascade) {
            NxSyncResyncHandler h = handler;
            if (h != null) {
                h.onResync(entityName, pks, cascade);
            }
        }

        @Override
        public void registerTrigger(String entityName, NxSyncTrigger trigger) {}

        @Override
        public void registerResyncHandler(NxSyncResyncHandler h) {
            this.handler = h;
        }
    }
}
