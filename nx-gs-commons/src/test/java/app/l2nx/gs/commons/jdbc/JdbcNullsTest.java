package app.l2nx.gs.commons.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class JdbcNullsTest {

    @Test
    void nullableInt_shouldReturnNull_whenWasNull() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getInt("col")).thenReturn(0);
        when(rs.wasNull()).thenReturn(true);

        assertNull(JdbcNulls.nullableInt(rs, "col"));
    }

    @Test
    void nullableInt_shouldReturnValue_whenNotNull() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getInt("col")).thenReturn(42);
        when(rs.wasNull()).thenReturn(false);

        assertEquals(Integer.valueOf(42), JdbcNulls.nullableInt(rs, "col"));
    }

    @Test
    void nullableInt_shouldReturnZero_whenNotNullAndValueIsZero() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getInt("col")).thenReturn(0);
        when(rs.wasNull()).thenReturn(false);

        // wasNull == false dominates — real 0 is preserved, not coerced to null.
        assertEquals(Integer.valueOf(0), JdbcNulls.nullableInt(rs, "col"));
    }

    @Test
    void nullableLong_shouldReturnNull_whenWasNull() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("col")).thenReturn(0L);
        when(rs.wasNull()).thenReturn(true);

        assertNull(JdbcNulls.nullableLong(rs, "col"));
    }

    @Test
    void nullableLong_shouldReturnValue_whenNotNull() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("col")).thenReturn(123L);
        when(rs.wasNull()).thenReturn(false);

        assertEquals(Long.valueOf(123L), JdbcNulls.nullableLong(rs, "col"));
    }

    @Test
    void nullableLong_shouldReturnZero_whenNotNullAndValueIsZero() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("col")).thenReturn(0L);
        when(rs.wasNull()).thenReturn(false);

        assertEquals(Long.valueOf(0L), JdbcNulls.nullableLong(rs, "col"));
    }
}
