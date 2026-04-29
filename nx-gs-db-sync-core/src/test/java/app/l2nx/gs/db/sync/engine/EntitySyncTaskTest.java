package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.sync.db.ClanDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntitySyncTaskTest {

    @Test
    void runCycle_shouldReturnDegraded_whenTopicMissing() throws SQLException {
        JdbcConnectionSource source = mock(JdbcConnectionSource.class);
        // No borrow expected — degraded short-circuits before getConnection.

        EntitySyncTask task = new EntitySyncTask(
                clanMapping(),
                source,
                new SnapshotStore(),
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                new SyncEventPublisher(neverCalledSender()),
                entityName -> null,
                EngineConfig.defaults());

        CycleResult result = task.runCycle();

        assertEquals(EntityState.DEGRADED, result.state());
        assertEquals(0L, result.created());
        assertEquals(0L, result.updated());
        assertEquals(0L, result.deleted());
    }

    @Test
    void runCycle_shouldReturnDegraded_whenJdbcBorrowFails() throws SQLException {
        JdbcConnectionSource source = mock(JdbcConnectionSource.class);
        when(source.getConnection()).thenThrow(new SQLException("pool exhausted"));

        EntitySyncTask task = new EntitySyncTask(
                clanMapping(),
                source,
                new SnapshotStore(),
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                new SyncEventPublisher(neverCalledSender()),
                entity -> "bohpts.gs.sync.clans",
                EngineConfig.defaults());

        CycleResult result = task.runCycle();

        assertEquals(EntityState.DEGRADED, result.state());
    }

    @Test
    void runCycle_shouldReturnDegraded_whenJdbcSourceThrowsRuntime() throws SQLException {
        JdbcConnectionSource source = mock(JdbcConnectionSource.class);
        when(source.getConnection()).thenThrow(new IllegalStateException("buggy spi"));

        EntitySyncTask task = new EntitySyncTask(
                clanMapping(),
                source,
                new SnapshotStore(),
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                new SyncEventPublisher(neverCalledSender()),
                entity -> "bohpts.gs.sync.clans",
                EngineConfig.defaults());

        CycleResult result = task.runCycle();

        assertEquals(EntityState.DEGRADED, result.state());
    }

    private static KafkaSender neverCalledSender() {
        return (topic, key, value, callback) -> {
            throw new AssertionError("KafkaSender must not be invoked when entity is degraded pre-cycle");
        };
    }

    private static EntityMapping<ClanDto> clanMapping() {
        return new EntityMapping<ClanDto>() {
            @Override
            public String entityName() {
                return "clan";
            }

            @Override
            public String tableName() {
                return "clan_data";
            }

            @Override
            public String pkColumn() {
                return "clan_id";
            }

            @Override
            public List<String> hashedColumns() {
                return Arrays.asList("clan_name", "clan_level");
            }

            @Override
            public ClanDto mapRow(ResultSet rs) throws SQLException {
                return ClanDto.builder()
                        .clanId(rs.getLong("clan_id"))
                        .clanName(rs.getString("clan_name"))
                        .clanLevel(rs.getInt("clan_level"))
                        .build();
            }

            @Override
            public Class<ClanDto> dtoType() {
                return ClanDto.class;
            }
        };
    }

    @SuppressWarnings("unused")
    private static RecordMetadata fakeMetadata() {
        return new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);
    }

    @SuppressWarnings("unused")
    private static Connection mockConnection() {
        return mock(Connection.class);
    }
}
