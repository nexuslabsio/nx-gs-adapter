package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.JdbcDialect;
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
    void hashPrimary_shouldExecuteWindowedCrcQuery() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong(1)).thenReturn(10L, 20L);
        when(rs.getLong(2)).thenReturn(111L, 222L);

        Long2IntMap result = new Phase1Hasher().hashPrimary(
                conn, new Window(0L, 100L), primary, 5, 10_000, JdbcDialect.OTHER);

        assertEquals(2, result.size());
        assertEquals(111, result.get(10L));
        assertEquals(222, result.get(20L));
        verify(ps).setQueryTimeout(5);
        verify(ps).setFetchSize(10_000);
        verify(ps).setLong(1, 0L);
        verify(ps).setLong(2, 100L);
    }

    @Test
    void hashPrimary_shouldUseStreamingFetchSize_whenDialectIsMysql() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        new Phase1Hasher().hashPrimary(conn, new Window(0L, 100L), primary, 5, 10_000, JdbcDialect.MYSQL);

        verify(ps).setFetchSize(Integer.MIN_VALUE);
    }

    @Test
    void hashChild_shouldReturnXorAggregatePerFk() throws SQLException {
        ChildSource<?> child = stubChild("clan_skills", "clan_id",
                Arrays.asList("skill_id", "skill_level"));
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong(1)).thenReturn(1L, 2L);
        when(rs.getLong(2)).thenReturn(0xAAAAL, 0xBBBBL);

        Long2IntMap result = new Phase1Hasher().hashChild(
                conn, new Window(0L, 100L), child, 5, 10_000, JdbcDialect.OTHER);

        assertEquals(2, result.size());
        assertEquals(0xAAAA, result.get(1L));
        assertEquals(0xBBBB, result.get(2L));
    }

    @Test
    void hashPrimary_shouldPropagateSQLException_whenQueryThrows() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLException("query failed"));

        SQLException thrown = assertThrows(SQLException.class,
                () -> new Phase1Hasher().hashPrimary(conn, new Window(0L, 100L), primary, 5, 10_000, JdbcDialect.OTHER));
        assertEquals("query failed", thrown.getMessage());
    }

    @Test
    void hashPrimary_shouldPropagateSQLTimeoutException_distinctFromGenericSQLException() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLTimeoutException("timeout"));

        assertThrows(SQLTimeoutException.class,
                () -> new Phase1Hasher().hashPrimary(conn, new Window(0L, 100L), primary, 5, 10_000, JdbcDialect.OTHER));
    }

    @Test
    void hashPrimary_shouldNarrowCrc_forValuesAboveIntegerMaxValue() throws SQLException {
        PrimarySource<?> primary = stubPrimary("clan_data", "clan_id",
                Arrays.asList("clan_name", "clan_level"));
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong(1)).thenReturn(42L);
        long crc32Unsigned = 3_735_928_559L; // 0xDEADBEEF
        when(rs.getLong(2)).thenReturn(crc32Unsigned);

        Long2IntMap result = new Phase1Hasher().hashPrimary(
                conn, new Window(0L, 100L), primary, 5, 10_000, JdbcDialect.OTHER);

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
