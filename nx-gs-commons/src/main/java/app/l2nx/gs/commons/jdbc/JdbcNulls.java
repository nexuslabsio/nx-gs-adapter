package app.l2nx.gs.commons.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Null-aware {@link ResultSet} readers. Returns {@code null} when the source
 * column is SQL NULL (per {@link ResultSet#wasNull()}), otherwise the boxed
 * value. Necessary because {@code rs.getInt} / {@code rs.getLong} return
 * {@code 0} for SQL NULL — without {@code wasNull()} the caller cannot
 * distinguish a real {@code 0} from a missing value.
 *
 * <p>The {@code instantFromEpochMillis*} helpers are the canonical entry
 * point for reading timestamp columns into wire DTO fields — wire DTOs use
 * exclusively {@link java.time.Instant} (UTC implicit by construction), so
 * schema providers MUST go through these helpers instead of
 * {@code rs.getTimestamp(...).toLocalDateTime()} or any other timezone-
 * sensitive path. {@link Instant#ofEpochMilli(long)} produces an Instant
 * with no host-timezone leakage; the platform-wide static check in
 * {@code WireTimestampConformanceTest} enforces that no wire DTO field
 * uses {@code OffsetDateTime} / {@code ZonedDateTime} / {@code LocalDateTime}
 * / {@code Date} / {@code Calendar}.
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

    /**
     * Reads an epoch-millis timestamp column as a UTC {@link Instant}.
     * {@code null} when the source value is SQL NULL.
     *
     * <p>Use for timestamp columns where {@code 0} is a legitimate epoch
     * value. When the source uses {@code 0} as a sentinel for "not set",
     * call {@link #instantFromEpochMillisOrSentinel} instead.</p>
     */
    public static @Nullable Instant nullableInstantFromEpochMillis(ResultSet rs, String column) throws SQLException {
        long raw = rs.getLong(column);
        return rs.wasNull() ? null : Instant.ofEpochMilli(raw);
    }

    /**
     * Reads an epoch-millis timestamp column as a UTC {@link Instant}.
     * {@code null} when the source value is SQL NULL OR equals
     * {@code sentinel}. Use when the source schema encodes "not set" as a
     * specific in-band value (often {@code 0}) rather than SQL NULL.
     */
    public static @Nullable Instant instantFromEpochMillisOrSentinel(ResultSet rs, String column, long sentinel)
            throws SQLException {
        long raw = rs.getLong(column);
        if (rs.wasNull() || raw == sentinel) return null;
        return Instant.ofEpochMilli(raw);
    }
}
