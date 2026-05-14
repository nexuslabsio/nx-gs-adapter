package app.l2nx.gs.db.sync.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/**
 * JDBC driver family detected from the connection URL. Drives fetch-size
 * semantics, which differ materially across drivers:
 *
 * <ul>
 *     <li>{@link #MYSQL} — MySQL Connector/J ignores positive {@code fetchSize}
 *     and buffers the entire result set in the client unless
 *     {@code fetchSize == Integer.MIN_VALUE} (streaming row-by-row,
 *     forward-only).</li>
 *     <li>{@link #MARIADB} — MariaDB Connector/J 3.x validates
 *     {@code fetchSize >= 0} and throws {@code SQLException: invalid fetch size}
 *     on the MySQL streaming sentinel. Pass a positive hint; for true
 *     server-side cursors add {@code useCursorFetch=true} to the JDBC URL.</li>
 *     <li>{@link #POSTGRES} — pgjdbc on {@code autoCommit=false} treats
 *     {@code fetchSize=N} as a server-side cursor batch.</li>
 *     <li>{@link #OTHER} — pass {@code fetchSize} as a generic hint;
 *     the driver may or may not honor it.</li>
 * </ul>
 */
public enum JdbcDialect {
    MYSQL,
    MARIADB,
    POSTGRES,
    OTHER;

    /**
     * Detect dialect from {@code conn.getMetaData().getURL()}. Returns
     * {@link #OTHER} on any error or unknown URL prefix — detection failure
     * is not fatal, just degrades to the generic fetch-size hint path.
     */
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
