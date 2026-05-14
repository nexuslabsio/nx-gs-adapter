package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.JdbcDialect;
import it.unimi.dsi.fastutil.longs.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class Phase2FetcherTest {

    @Test
    void buildSql_shouldEmitInClauseWithRequestedPlaceholderCount() {
        String sql = Phase2Fetcher.buildSql("clan_data", "clan_id", 3);

        assertEquals("SELECT * FROM clan_data WHERE clan_id IN (?, ?, ?)", sql);
    }

    @Test
    void buildSql_shouldEmitSingleInClause_whenOnePlaceholder() {
        String sql = Phase2Fetcher.buildSql("clan_data", "clan_id", 1);

        assertEquals("SELECT * FROM clan_data WHERE clan_id IN (?)", sql);
    }

    @Test
    void buildSql_shouldThrow_whenPlaceholderCountZeroOrNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> Phase2Fetcher.buildSql("t", "id", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Phase2Fetcher.buildSql("t", "id", -1));
    }

    @Test
    void chunkSize_shouldBe1000_perCdcEngineSpec() {
        assertEquals(1000, Phase2Fetcher.CHUNK_SIZE);
    }

    @Test
    void toList_shouldCopyKeysFromSet() {
        LongSet keys = new LongOpenHashSet();
        keys.add(10L);
        keys.add(20L);
        keys.add(30L);

        LongList list = Phase2Fetcher.toList(keys);

        assertEquals(3, list.size());
        assertTrue(list.contains(10L));
        assertTrue(list.contains(20L));
        assertTrue(list.contains(30L));
    }

    @Test
    void fetchPrimary_shouldPrepareStatementOnce_andPadLastChunk() throws SQLException {
        PrimarySource<TestRow> primary = clanPrimary();
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong("clan_id")).thenReturn(1L, 2L);
        when(rs.getString("clan_name")).thenReturn("A", "B");
        when(rs.getInt("clan_level")).thenReturn(5, 7);

        LongList pks = new LongArrayList(new long[]{1L, 2L, 3L});

        Long2ObjectMap<Object> result = new Phase2Fetcher().fetchPrimary(
                conn, primary, pks, 5, 10_000, JdbcDialect.OTHER);

        verify(conn, times(1)).prepareStatement(anyString());
        verify(ps, times(Phase2Fetcher.CHUNK_SIZE))
                .setLong(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyLong());
        verify(ps).setLong(1, 1L);
        verify(ps).setLong(2, 2L);
        verify(ps).setLong(3, 3L);
        verify(ps, times(Phase2Fetcher.CHUNK_SIZE - 2))
                .setLong(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.eq(3L));
        verify(ps).setQueryTimeout(5);
        verify(ps).setFetchSize(10_000);

        assertEquals(2, result.size());
    }

    @Test
    void fetchChild_shouldGroupRowsByFk() throws SQLException {
        ChildSource<TestSkill> child = skillChild();
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getLong("clan_id")).thenReturn(1L, 1L, 2L);
        when(rs.getInt("skill_id")).thenReturn(101, 102, 201);
        when(rs.getInt("skill_level")).thenReturn(1, 2, 1);

        LongList fks = new LongArrayList(new long[]{1L, 2L, 3L});

        Long2ObjectMap<List<Object>> result = new Phase2Fetcher().fetchChild(
                conn, child, fks, 5, 10_000, JdbcDialect.OTHER);

        assertEquals(2, result.size(), "FK 3 absent from result map (no children)");
        assertEquals(2, result.get(1L).size(), "FK 1 has 2 skills");
        assertEquals(1, result.get(2L).size(), "FK 2 has 1 skill");
        assertNull(result.get(3L), "FK 3 absent — caller substitutes empty list");
    }

    @Test
    void fetchPrimary_shouldPropagateSQLException_whenChunkQueryThrows() throws SQLException {
        PrimarySource<TestRow> primary = clanPrimary();
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLException("boom"));

        LongList pks = new LongArrayList(new long[]{1L, 2L});

        assertThrows(SQLException.class,
                () -> new Phase2Fetcher().fetchPrimary(conn, primary, pks, 5, 10_000, JdbcDialect.OTHER));
    }

    @Test
    void fetchPrimary_shouldReturnEmptyMap_whenPksEmpty() throws SQLException {
        Connection conn = mock(Connection.class);

        Long2ObjectMap<Object> result = new Phase2Fetcher().fetchPrimary(
                conn, clanPrimary(), new LongArrayList(), 5, 10_000, JdbcDialect.OTHER);

        assertTrue(result.isEmpty());
        verify(conn, times(0)).prepareStatement(anyString());
    }

    @Test
    void fetchChild_shouldReturnEmptyMap_whenFksEmpty() throws SQLException {
        Connection conn = mock(Connection.class);

        Long2ObjectMap<List<Object>> result = new Phase2Fetcher().fetchChild(
                conn, skillChild(), new LongArrayList(), 5, 10_000, JdbcDialect.OTHER);

        assertTrue(result.isEmpty());
        verify(conn, times(0)).prepareStatement(anyString());
    }

    private static PrimarySource<TestRow> clanPrimary() {
        return new PrimarySource<TestRow>() {
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
            public TestRow mapRow(ResultSet rs) throws SQLException {
                return new TestRow(rs.getLong("clan_id"), rs.getString("clan_name"), rs.getInt("clan_level"));
            }
        };
    }

    private static ChildSource<TestSkill> skillChild() {
        return new ChildSource<TestSkill>() {
            @Override
            public String tableName() {
                return "clan_skills";
            }

            @Override
            public String fkColumn() {
                return "clan_id";
            }

            @Override
            public List<String> hashedColumns() {
                return Arrays.asList("skill_id", "skill_level");
            }

            @Override
            public TestSkill mapRow(ResultSet rs) throws SQLException {
                return new TestSkill(rs.getInt("skill_id"), rs.getInt("skill_level"));
            }
        };
    }

    private static final class TestRow {
        final long clanId;
        final String clanName;
        final int clanLevel;

        TestRow(long clanId, String clanName, int clanLevel) {
            this.clanId = clanId;
            this.clanName = clanName;
            this.clanLevel = clanLevel;
        }
    }

    private static final class TestSkill {
        final int skillId;
        final int skillLevel;

        TestSkill(int skillId, int skillLevel) {
            this.skillId = skillId;
            this.skillLevel = skillLevel;
        }
    }
}
