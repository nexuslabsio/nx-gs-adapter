package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
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
    void buildPrimarySql_shouldEmitCrc32ConcatWs_inDeclaredColumnOrder() {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level", "leader_id"));

        String sql = Phase1Hasher.buildPrimarySql(primary);

        assertEquals("SELECT clan_id, CRC32(CONCAT_WS(',', clan_name, clan_level, leader_id)) "
                + "FROM clan_data WHERE clan_id BETWEEN ? AND ?", sql);
    }

    @Test
    void buildPrimarySql_shouldHandleSingleHashedColumn() {
        PrimarySource<?> primary = stubPrimary("foo_t", "id",
                Collections.singletonList("name"));

        assertTrue(Phase1Hasher.buildPrimarySql(primary).contains("CRC32(CONCAT_WS(',', name))"));
    }

    @Test
    void buildPrimarySql_shouldThrow_whenHashedColumnsEmpty() {
        PrimarySource<?> primary = stubPrimary("foo_t", "id", Collections.emptyList());

        assertThrows(IllegalArgumentException.class, () -> Phase1Hasher.buildPrimarySql(primary));
    }

    @Test
    void buildPrimarySql_shouldThrow_whenHashedColumnsNull() {
        PrimarySource<?> primary = stubPrimary("foo_t", "id", null);

        assertThrows(IllegalArgumentException.class, () -> Phase1Hasher.buildPrimarySql(primary));
    }

    @Test
    void buildChildSql_shouldEmitBitXorOverCrc32_groupedByFk() {
        ChildSource<?> child = stubChild("clan_skills", "clan_id",
                Arrays.asList("skill_id", "skill_level"));

        String sql = Phase1Hasher.buildChildSql(child);

        assertEquals("SELECT clan_id, BIT_XOR(CRC32(CONCAT_WS(',', skill_id, skill_level))) "
                + "FROM clan_skills WHERE clan_id BETWEEN ? AND ? "
                + "GROUP BY clan_id", sql);
    }

    @Test
    void buildChildSql_shouldThrow_whenHashedColumnsEmpty() {
        ChildSource<?> child = stubChild("clan_skills", "clan_id", Collections.emptyList());

        assertThrows(IllegalArgumentException.class, () -> Phase1Hasher.buildChildSql(child));
    }

    @Test
    void hashPrimary_shouldRunInsideConsistentSnapshotTxn_andRestoreAutoCommit() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
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

        Long2IntMap result = new Phase1Hasher().hashPrimary(
                new Window(0L, 100L), primary, conn, 5);

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
    void hashChild_shouldRunInsideConsistentSnapshotTxn_andReturnXorAggregatePerFk() throws SQLException {
        ChildSource<?> child = stubChild("clan_skills", "clan_id",
                Arrays.asList("skill_id", "skill_level"));
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        // GROUP BY fk: 2 rows, FK=1 has aggregate 0xAAAA, FK=2 has 0xBBBB
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong(1)).thenReturn(1L, 2L);
        when(rs.getLong(2)).thenReturn(0xAAAAL, 0xBBBBL);

        Long2IntMap result = new Phase1Hasher().hashChild(
                new Window(0L, 100L), child, conn, 5);

        assertEquals(2, result.size());
        assertEquals(0xAAAA, result.get(1L));
        assertEquals(0xBBBB, result.get(2L));
        verify(init).execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
        verify(conn).commit();
    }

    @Test
    void hashPrimary_shouldRollbackAndRestoreAutoCommit_whenQueryThrows() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLException("query failed"));

        SQLException thrown = assertThrows(SQLException.class,
                () -> new Phase1Hasher().hashPrimary(new Window(0L, 100L), primary, conn, 5));
        assertEquals("query failed", thrown.getMessage());

        verify(conn).rollback();
        verify(conn, never()).commit();
        verify(conn).setAutoCommit(true); // restored even after rollback
    }

    @Test
    void hashPrimary_shouldPropagateSQLTimeoutException_distinctFromGenericSQLException() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.getAutoCommit()).thenReturn(false);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLTimeoutException("timeout"));

        assertThrows(SQLTimeoutException.class,
                () -> new Phase1Hasher().hashPrimary(new Window(0L, 100L), primary, conn, 5));

        // Engine relies on SQLTimeoutException being a separate type so EntitySyncTask
        // can branch on it (skip-this-window) vs generic SQLException (abort cycle).
        verify(conn).rollback();
        // setAutoCommit(false) called twice: once to start the txn, once to restore prior value (also false).
        verify(conn, times(2)).setAutoCommit(false);
    }

    @Test
    void hashPrimary_shouldNarrowCrc_forValuesAboveIntegerMaxValue() throws SQLException {
        // MySQL's CRC32 returns BIGINT UNSIGNED (0..2^32-1); the engine narrows
        // via (int) which preserves all 32 bits even when the long value exceeds
        // Integer.MAX_VALUE. Without the cast the diff would mis-classify rows.
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
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

        Long2IntMap result = new Phase1Hasher().hashPrimary(
                new Window(0L, 100L), primary, conn, 5);

        assertEquals((int) crc32Unsigned, result.get(42L),
                "CRC32 narrowing must preserve all 32 bits");
    }

    private static PrimarySource<Object> stubPrimary(String table, String pk, List<String> hashed) {
        return new PrimarySource<Object>() {
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
            public Object mapRow(ResultSet rs) {
                return null;
            }
        };
    }

    private static ChildSource<Object> stubChild(String table, String fk, List<String> hashed) {
        return new ChildSource<Object>() {
            @Override
            public String tableName() {
                return table;
            }

            @Override
            public String fkColumn() {
                return fk;
            }

            @Override
            public List<String> hashedColumns() {
                return hashed;
            }

            @Override
            public Object mapRow(ResultSet rs) {
                return null;
            }
        };
    }
}
