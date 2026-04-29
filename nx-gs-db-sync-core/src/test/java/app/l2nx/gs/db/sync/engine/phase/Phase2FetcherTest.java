package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.kafka.sync.db.ClanDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import it.unimi.dsi.fastutil.longs.*;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class Phase2FetcherTest {

    @Test
    void buildSql_shouldEmitInClauseWithRequestedPlaceholderCount() {
        EntityMapping<Object> mapping = mappingOf();

        String sql = Phase2Fetcher.buildSql(mapping, 3);

        assertEquals("SELECT * FROM clan_data WHERE clan_id IN (?, ?, ?)", sql);
    }

    @Test
    void buildSql_shouldEmitSingleInClause_whenOnePlaceholder() {
        EntityMapping<Object> mapping = mappingOf();

        String sql = Phase2Fetcher.buildSql(mapping, 1);

        assertEquals("SELECT * FROM clan_data WHERE clan_id IN (?)", sql);
    }

    @Test
    void buildSql_shouldThrow_whenPlaceholderCountZeroOrNegative() {
        EntityMapping<Object> mapping = mappingOf();

        assertThrows(IllegalArgumentException.class, () -> Phase2Fetcher.buildSql(mapping, 0));
        assertThrows(IllegalArgumentException.class, () -> Phase2Fetcher.buildSql(mapping, -1));
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

        it.unimi.dsi.fastutil.longs.LongList list = Phase2Fetcher.toList(keys);

        assertEquals(3, list.size());
        assertTrue(list.contains(10L));
        assertTrue(list.contains(20L));
        assertTrue(list.contains(30L));
    }

    @Test
    void fetch_shouldPrepareStatementOnce_andPadLastChunk() throws SQLException {
        EntityMapping<ClanDto> mapping = clanMapping();
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong("clan_id")).thenReturn(1L, 2L);
        when(rs.getString("clan_name")).thenReturn("A", "B");
        when(rs.getInt("clan_level")).thenReturn(5, 7);
        when(rs.getLong("leader_id")).thenReturn(0L, 0L);
        when(rs.getLong("ally_id")).thenReturn(0L, 0L);

        // 3 PKs → one chunk, padded to CHUNK_SIZE so the SQL string is the
        // CHUNK_SIZE-placeholder version (cache-stable).
        LongList pks = new LongArrayList(new long[]{1L, 2L, 3L});

        Long2ObjectMap<ClanDto> result = new Phase2Fetcher().fetch(mapping, pks, conn, 5);

        verify(conn, times(1)).prepareStatement(anyString());
        // CHUNK_SIZE setLong calls — last (CHUNK_SIZE-3) padded with the final PK
        verify(ps, times(Phase2Fetcher.CHUNK_SIZE)).setLong(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyLong());
        verify(ps).setLong(1, 1L);
        verify(ps).setLong(2, 2L);
        verify(ps).setLong(3, 3L);
        // Padding: indices 4..CHUNK_SIZE all bound to the last real PK (3L)
        verify(ps, times(Phase2Fetcher.CHUNK_SIZE - 2)).setLong(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.eq(3L));
        verify(ps).setQueryTimeout(5);
        verify(conn).commit();

        assertEquals(2, result.size());
    }

    @Test
    void fetch_shouldRollback_whenChunkQueryThrows() throws SQLException {
        EntityMapping<ClanDto> mapping = clanMapping();
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLException("boom"));

        LongList pks = new LongArrayList(new long[]{1L, 2L});

        assertThrows(SQLException.class, () -> new Phase2Fetcher().fetch(mapping, pks, conn, 5));

        verify(conn).rollback();
    }

    @Test
    void fetch_shouldReturnEmptyMap_whenPksEmpty() throws SQLException {
        Connection conn = mock(Connection.class);

        Long2ObjectMap<ClanDto> result = new Phase2Fetcher().fetch(
                clanMapping(), new LongArrayList(), conn, 5);

        assertTrue(result.isEmpty());
        verify(conn, times(0)).setAutoCommit(false);
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

    private static EntityMapping<Object> mappingOf() {
        return new EntityMapping<Object>() {
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
