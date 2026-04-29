package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class Phase1HasherTest {

    @Test
    void buildSql_shouldEmitCrc32ConcatWs_inDeclaredColumnOrder() {
        EntityMapping<Object> mapping = mappingOf("clan", "clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level", "leader_id"));

        String sql = Phase1Hasher.buildSql(mapping);

        assertEquals("SELECT clan_id, CRC32(CONCAT_WS(',', clan_name, clan_level, leader_id)) "
                + "FROM clan_data WHERE clan_id BETWEEN ? AND ?", sql);
    }

    @Test
    void buildSql_shouldHandleSingleHashedColumn() {
        EntityMapping<Object> mapping = mappingOf("foo", "foo_t", "id",
                Collections.singletonList("name"));

        String sql = Phase1Hasher.buildSql(mapping);

        assertTrue(sql.contains("CRC32(CONCAT_WS(',', name))"));
    }

    @Test
    void buildSql_shouldThrow_whenHashedColumnsEmpty() {
        EntityMapping<Object> mapping = mappingOf("foo", "foo_t", "id",
                Collections.emptyList());

        assertThrows(IllegalArgumentException.class, () -> Phase1Hasher.buildSql(mapping));
    }

    @Test
    void buildSql_shouldThrow_whenHashedColumnsNull() {
        EntityMapping<Object> mapping = mappingOf("foo", "foo_t", "id", null);

        assertThrows(IllegalArgumentException.class, () -> Phase1Hasher.buildSql(mapping));
    }

    @Test
    void hash_shouldRunInsideConsistentSnapshotTxn_andRestoreAutoCommit() throws SQLException {
        EntityMapping<Object> mapping = clanMapping();
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong(1)).thenReturn(10L, 20L);
        when(rs.getLong(2)).thenReturn(111L, 222L);

        Long2IntMap result = new Phase1Hasher().hash(
                new Window(0L, 100L), mapping, conn, 5);

        assertEquals(2, result.size());
        assertEquals(111, result.get(10L));
        assertEquals(222, result.get(20L));

        verify(conn).setAutoCommit(false);
        verify(init).execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
        verify(ps).setQueryTimeout(5);
        verify(ps).setLong(1, 0L);
        verify(ps).setLong(2, 100L);
        verify(conn).commit();
        verify(conn, never()).rollback();
        verify(conn).setAutoCommit(true);
    }

    @Test
    void hash_shouldRollbackAndRestoreAutoCommit_whenQueryThrows() throws SQLException {
        EntityMapping<Object> mapping = clanMapping();
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLException("query failed"));

        SQLException thrown = assertThrows(SQLException.class,
                () -> new Phase1Hasher().hash(new Window(0L, 100L), mapping, conn, 5));
        assertEquals("query failed", thrown.getMessage());

        verify(conn).rollback();
        verify(conn, never()).commit();
        verify(conn).setAutoCommit(true); // restored even after rollback
    }

    @Test
    void hash_shouldPropagateSQLTimeoutException_distinctFromGenericSQLException() throws SQLException {
        EntityMapping<Object> mapping = clanMapping();
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.getAutoCommit()).thenReturn(false);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLTimeoutException("timeout"));

        assertThrows(SQLTimeoutException.class,
                () -> new Phase1Hasher().hash(new Window(0L, 100L), mapping, conn, 5));

        // Engine relies on SQLTimeoutException being a separate type so EntitySyncTask
        // can branch on it (skip-this-window) vs generic SQLException (abort cycle).
        verify(conn).rollback();
        // setAutoCommit(false) called twice: once to start the txn, once to restore prior value (also false).
        verify(conn, times(2)).setAutoCommit(false);
    }

    @Test
    void hash_shouldNarrowCrc_forValuesAboveIntegerMaxValue() throws SQLException {
        // MySQL's CRC32 returns BIGINT UNSIGNED (0..2^32-1); the engine narrows
        // via (int) which preserves all 32 bits even when the long value exceeds
        // Integer.MAX_VALUE. Without the cast the diff would mis-classify rows.
        EntityMapping<Object> mapping = clanMapping();
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong(1)).thenReturn(42L);
        // CRC32 value 0xDEADBEEF = 3735928559L > Integer.MAX_VALUE
        long crc32Unsigned = 3_735_928_559L;
        when(rs.getLong(2)).thenReturn(crc32Unsigned);

        Long2IntMap result = new Phase1Hasher().hash(
                new Window(0L, 100L), mapping, conn, 5);

        assertEquals((int) crc32Unsigned, result.get(42L),
                "CRC32 narrowing must preserve all 32 bits");
    }

    private static EntityMapping<Object> clanMapping() {
        return mappingOf("clan", "clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
    }

    private static EntityMapping<Object> mappingOf(String entity, String table, String pk,
                                                   List<String> hashed) {
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return entity;
            }

            @Override
            public String tableName() {
                return table;
            }

            @Override
            public String pkColumn() {
                return pk;
            }

            @Override
            public List<String> hashedColumns() {
                return hashed;
            }

            @Override
            public Object mapRow(ResultSet rs) throws SQLException {
                return null;
            }

            @Override
            public Class<Object> dtoType() {
                return Object.class;
            }
        };
    }
}
