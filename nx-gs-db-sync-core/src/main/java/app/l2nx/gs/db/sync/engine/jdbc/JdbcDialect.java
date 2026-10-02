package app.l2nx.gs.db.sync.engine.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Driver family detected from the URL; fetch-size semantics differ: MySQL needs {@code Integer.MIN_VALUE} to stream, MariaDB 3.x throws on it
 * (use a positive hint, plus {@code useCursorFetch=true} for server cursors), pgjdbc needs {@code autoCommit=false} for cursor batches.
 */
public enum JdbcDialect {
    MYSQL,
    MARIADB,
    POSTGRES,
    OTHER;

    /** Returns {@link #OTHER} on any error or unknown URL; detection failure is not fatal. */
    public static JdbcDialect detect(Connection conn) {
        try {
            String url = conn.getMetaData().getURL();
            if (url == null) {
                return OTHER;
            }
            String lower = url.toLowerCase(Locale.ROOT);
            if (lower.startsWith("jdbc:mariadb:")) {
                return MARIADB;
            }
            if (lower.startsWith("jdbc:mysql:")) {
                return MYSQL;
            }
            if (lower.startsWith("jdbc:postgresql:") || lower.startsWith("jdbc:postgres:")) {
                return POSTGRES;
            }
            return OTHER;
        } catch (SQLException e) {
            return OTHER;
        }
    }
}
