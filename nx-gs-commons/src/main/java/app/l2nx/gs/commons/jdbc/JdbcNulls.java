package app.l2nx.gs.commons.jdbc;

import org.jspecify.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Null-aware {@link ResultSet} readers. Returns {@code null} when the source
 * column is SQL NULL (per {@link ResultSet#wasNull()}), otherwise the boxed
 * value. Necessary because {@code rs.getInt} / {@code rs.getLong} return
 * {@code 0} for SQL NULL — without {@code wasNull()} the caller cannot
 * distinguish a real {@code 0} from a missing value.
 */
public final class JdbcNulls {

    private JdbcNulls() {
    }

    public static @Nullable Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int raw = rs.getInt(column);
        return rs.wasNull() ? null : raw;
    }

    public static @Nullable Long nullableLong(ResultSet rs, String column) throws SQLException {
        long raw = rs.getLong(column);
        return rs.wasNull() ? null : raw;
    }
}
