package app.l2nx.gs.commons.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Null for SQL NULL: rs.getInt/getLong return 0 and cannot be told from a real 0 without wasNull().
 * Timestamps go through Instant.ofEpochMilli so no host timezone leaks into wire DTOs (Instant only).
 */
public final class JdbcNulls {

    private JdbcNulls() {}

    public static @Nullable Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int raw = rs.getInt(column);
        return rs.wasNull() ? null : raw;
    }

    public static @Nullable Long nullableLong(ResultSet rs, String column) throws SQLException {
        long raw = rs.getLong(column);
        return rs.wasNull() ? null : raw;
    }

    /** Null for SQL NULL; use the OrSentinel variant when 0 means "not set". */
    public static @Nullable Instant nullableInstantFromEpochMillis(ResultSet rs, String column) throws SQLException {
        long raw = rs.getLong(column);
        return rs.wasNull() ? null : Instant.ofEpochMilli(raw);
    }

    /** Null for SQL NULL or a value equal to {@code sentinel}. */
    public static @Nullable Instant instantFromEpochMillisOrSentinel(ResultSet rs, String column, long sentinel)
            throws SQLException {
        long raw = rs.getLong(column);
        if (rs.wasNull() || raw == sentinel) return null;
        return Instant.ofEpochMilli(raw);
    }
}
