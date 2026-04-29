package app.l2nx.gs.db.sync.engine.window;

import app.l2nx.gs.adapter.api.kafka.sync.db.ClanDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class WindowPlannerTest {

    @Test
    void divideRange_shouldReturnEmpty_whenMaxBelowMin() {
        assertTrue(WindowPlanner.divideRange(10, 5, 100).isEmpty());
    }

    @Test
    void divideRange_shouldReturnSingleWindow_whenSpanFitsInOneChunk() {
        List<Window> windows = WindowPlanner.divideRange(1, 100, 1000);

        assertEquals(1, windows.size());
        assertEquals(new Window(1, 100), windows.get(0));
    }

    @Test
    void divideRange_shouldReturnSingleRowWindow_whenMinEqualsMax() {
        List<Window> windows = WindowPlanner.divideRange(42, 42, 1000);

        assertEquals(1, windows.size());
        assertEquals(new Window(42, 42), windows.get(0));
    }

    @Test
    void divideRange_shouldChunkEvenly_whenSpanIsMultipleOfRowsPerWindow() {
        List<Window> windows = WindowPlanner.divideRange(1, 10, 5);

        assertEquals(2, windows.size());
        assertEquals(Arrays.asList(new Window(1, 5), new Window(6, 10)), windows);
    }

    @Test
    void divideRange_shouldHandleRemainder_whenSpanNotMultiple() {
        List<Window> windows = WindowPlanner.divideRange(1, 12, 5);

        assertEquals(3, windows.size());
        assertEquals(Arrays.asList(
                new Window(1, 5),
                new Window(6, 10),
                new Window(11, 12)), windows);
    }

    @Test
    void divideRange_shouldChunk12MItemsInto10Windows_whenRowsPerWindowMatches() {
        // bohpts x20 reference: 12.2M items split into 10 sliding windows of ~1.22M each.
        long min = 1L;
        long max = 12_200_000L;
        int rowsPerWindow = 1_220_000;

        List<Window> windows = WindowPlanner.divideRange(min, max, rowsPerWindow);

        assertEquals(10, windows.size());
        assertEquals(min, windows.get(0).fromPk());
        assertEquals(max, windows.get(windows.size() - 1).toPk());
        // Adjacency
        for (int i = 1; i < windows.size(); i++) {
            assertEquals(windows.get(i - 1).toPk() + 1L, windows.get(i).fromPk(),
                    "window " + i + " not adjacent to predecessor");
        }
    }

    @Test
    void divideRange_shouldNotOverflow_whenMaxNearLongMax() {
        long min = Long.MAX_VALUE - 10L;
        long max = Long.MAX_VALUE;

        List<Window> windows = WindowPlanner.divideRange(min, max, 5);

        // 11 rows → ceil(11/5) = 3 windows
        assertEquals(3, windows.size());
        assertEquals(max, windows.get(windows.size() - 1).toPk());
    }

    @Test
    void divideRange_shouldNotReturnSingleWindow_whenSpanOverflowsLongAddition() {
        // Pre-fix: returned a single Window covering the entire BIGINT range,
        // defeating chunking entirely. Post-fix: forces chunking — and because
        // 2^64 / Integer.MAX_VALUE far exceeds MAX_WINDOWS_PER_PLAN, the planner
        // hits the cap. Either branch is correct; what must NOT happen is the
        // silent single-window collapse the bug produced.
        Throwable thrown = null;
        try {
            WindowPlanner.divideRange(Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
        } catch (IllegalStateException ex) {
            thrown = ex;
        }
        assertNotNull(thrown,
                "full-BIGINT range must hit the windows cap, never collapse to a single window");
    }

    @Test
    void plan_shouldReturnEmpty_whenTableEmpty() throws SQLException {
        // MIN/MAX of an empty table both come back NULL — wasNull() == true after
        // each getLong. Plan must collapse to empty list (engine treats this as
        // "no windows this cycle", not as a single dummy window).
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getLong(1)).thenReturn(0L);
        when(rs.getLong(2)).thenReturn(0L);
        when(rs.wasNull()).thenReturn(true, true);

        List<Window> windows = new WindowPlanner().plan(clanMapping(), conn, 1000, 5);

        assertTrue(windows.isEmpty());
        verify(st).setQueryTimeout(5);
    }

    @Test
    void plan_shouldUseMinMaxFromConnection_andProduceContiguousWindows() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getLong(1)).thenReturn(1L);
        when(rs.getLong(2)).thenReturn(12L);
        when(rs.wasNull()).thenReturn(false, false);

        List<Window> windows = new WindowPlanner().plan(clanMapping(), conn, 5, 5);

        assertEquals(3, windows.size());
        assertEquals(new Window(1, 5), windows.get(0));
        assertEquals(new Window(6, 10), windows.get(1));
        assertEquals(new Window(11, 12), windows.get(2));
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
            public ClanDto mapRow(ResultSet r) {
                return null;
            }

            @Override
            public Class<ClanDto> dtoType() {
                return ClanDto.class;
            }
        };
    }

    @Test
    void divideRange_shouldThrow_whenWindowCountExceedsCap() {
        // 1M windows × rowsPerWindow=1 across a normal range → cap hit.
        Throwable t = assertThrowsOrNull(() ->
                WindowPlanner.divideRange(0L, WindowPlanner.MAX_WINDOWS_PER_PLAN + 100L, 1));
        assertNotNull(t, "expected IllegalStateException at cap");
    }

    private static Throwable assertThrowsOrNull(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
