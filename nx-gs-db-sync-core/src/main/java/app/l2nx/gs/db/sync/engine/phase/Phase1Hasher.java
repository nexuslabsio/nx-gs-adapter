package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.JdbcDialect;
import app.l2nx.gs.db.sync.engine.StatementRegistry;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongList;
import java.sql.*;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Phase 1 of the CRC32 CDC protocol. {@link #hashPrimary} returns (PK → CRC32);
 * {@link #hashChild} returns (parent PK → XOR-aggregate CRC32) — XOR is order-
 * insensitive so child row order has no bearing on the hash.
 *
 * <p>Both variants run against a {@link Connection} already inside the
 * caller's consistent-snapshot transaction. The engine opens one
 * transaction per window for primary + every child to keep the entire
 * window-CRC bit-exact under concurrent writes.</p>
 */
public final class Phase1Hasher {

    /**
     * Sentinel marking "no entry" — distinct from a legit CRC32 of 0.
     */
    public static final int MISSING_HASH = Integer.MIN_VALUE;

    public Long2IntMap hashPrimary(
            Connection conn,
            Window window,
            PrimarySource<?> primary,
            int queryTimeoutSeconds,
            int fetchSize,
            JdbcDialect dialect)
            throws SQLException {
        return hashPrimary(conn, window, primary, queryTimeoutSeconds, fetchSize, dialect, null);
    }

    public Long2IntMap hashPrimary(
            Connection conn,
            Window window,
            PrimarySource<?> primary,
            int queryTimeoutSeconds,
            int fetchSize,
            JdbcDialect dialect,
            @Nullable StatementRegistry registry)
            throws SQLException {
        if (window.targeted()) {
            LongList pks = window.pks();
            String sql = buildPrimaryInSql(primary, pks.size());
            return runInListHashQuery(conn, pks, sql, queryTimeoutSeconds, fetchSize, registry);
        }
        String sql = buildPrimarySql(primary);
        return runHashQuery(conn, window, sql, queryTimeoutSeconds, fetchSize, dialect, registry);
    }

    public Long2IntMap hashChild(
            Connection conn,
            Window window,
            ChildSource<?> child,
            int queryTimeoutSeconds,
            int fetchSize,
            JdbcDialect dialect)
            throws SQLException {
        return hashChild(conn, window, child, queryTimeoutSeconds, fetchSize, dialect, null);
    }

    public Long2IntMap hashChild(
            Connection conn,
            Window window,
            ChildSource<?> child,
            int queryTimeoutSeconds,
            int fetchSize,
            JdbcDialect dialect,
            @Nullable StatementRegistry registry)
            throws SQLException {
        if (window.targeted()) {
            LongList parentPks = window.pks();
            String sql = buildChildInSql(child, parentPks.size());
            return runInListHashQuery(conn, parentPks, sql, queryTimeoutSeconds, fetchSize, registry);
        }
        String sql = buildChildSql(child);
        return runHashQuery(conn, window, sql, queryTimeoutSeconds, fetchSize, dialect, registry);
    }

    private static Long2IntMap runHashQuery(
            Connection conn,
            Window window,
            String sql,
            int queryTimeoutSeconds,
            int fetchSize,
            JdbcDialect dialect,
            @Nullable StatementRegistry registry)
            throws SQLException {
        Long2IntOpenHashMap result = new Long2IntOpenHashMap();
        result.defaultReturnValue(MISSING_HASH);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (registry != null) {
                registry.set(ps);
            }
            try {
                ps.setQueryTimeout(queryTimeoutSeconds);
                applyFetchSize(ps, fetchSize, dialect);
                ps.setLong(1, window.fromPk());
                ps.setLong(2, window.toPk());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long pk = rs.getLong(1);
                        // CRC32 / BIT_XOR(CRC32) returns BIGINT UNSIGNED; narrow to int — bit-exact.
                        int crc = (int) rs.getLong(2);
                        result.put(pk, crc);
                    }
                }
            } finally {
                if (registry != null) {
                    registry.clear();
                }
            }
        }
        return result;
    }

    private static Long2IntMap runInListHashQuery(
            Connection conn,
            LongList keys,
            String sql,
            int queryTimeoutSeconds,
            int fetchSize,
            @Nullable StatementRegistry registry)
            throws SQLException {
        Long2IntOpenHashMap result = new Long2IntOpenHashMap();
        result.defaultReturnValue(MISSING_HASH);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (registry != null) {
                registry.set(ps);
            }
            try {
                ps.setQueryTimeout(queryTimeoutSeconds);
                // Bounded IN-list — plain fetch, no MySQL Integer.MIN_VALUE streaming cursor (range-scan only).
                ps.setFetchSize(fetchSize);
                for (int i = 0; i < keys.size(); i++) {
                    ps.setLong(i + 1, keys.getLong(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long pk = rs.getLong(1);
                        // CRC32 / BIT_XOR(CRC32) returns BIGINT UNSIGNED; narrow to int — bit-exact.
                        int crc = (int) rs.getLong(2);
                        result.put(pk, crc);
                    }
                }
            } finally {
                if (registry != null) {
                    registry.clear();
                }
            }
        }
        return result;
    }

    /**
     * MySQL Connector/J ignores positive {@code fetchSize} and only honors
     * {@code Integer.MIN_VALUE} (streaming row-by-row). MariaDB Connector/J 3.x
     * rejects the same sentinel ({@code SQLException: invalid fetch size}) —
     * see {@link JdbcDialect#MARIADB}. Pgjdbc and most other drivers treat
     * positive {@code fetchSize} as a server-side cursor batch hint.
     */
    static void applyFetchSize(Statement ps, int fetchSize, JdbcDialect dialect) throws SQLException {
        if (dialect == JdbcDialect.MYSQL) {
            ps.setFetchSize(Integer.MIN_VALUE);
        } else {
            ps.setFetchSize(fetchSize);
        }
    }

    static String buildPrimarySql(PrimarySource<?> primary) {
        List<String> hashed = primary.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException("PrimarySource " + primary.tableName() + " has no hashedColumns");
        }
        return "SELECT " + primary.pkColumn() + ", " + concatCrc32(hashed)
                + " FROM " + primary.tableName()
                + " WHERE " + primary.pkColumn() + " BETWEEN ? AND ?";
    }

    static String buildChildSql(ChildSource<?> child) {
        List<String> hashed = child.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException("ChildSource " + child.tableName() + " has no hashedColumns");
        }
        return "SELECT " + child.fkColumn() + ", BIT_XOR(" + concatCrc32(hashed) + ") "
                + "FROM " + child.tableName()
                + " WHERE " + child.fkColumn() + " BETWEEN ? AND ? "
                + "GROUP BY " + child.fkColumn();
    }

    static String buildPrimaryInSql(PrimarySource<?> primary, int placeholderCount) {
        List<String> hashed = primary.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException("PrimarySource " + primary.tableName() + " has no hashedColumns");
        }
        return "SELECT " + primary.pkColumn() + ", " + concatCrc32(hashed)
                + " FROM " + primary.tableName()
                + " WHERE " + primary.pkColumn() + " IN (" + placeholders(placeholderCount) + ")";
    }

    static String buildChildInSql(ChildSource<?> child, int placeholderCount) {
        List<String> hashed = child.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException("ChildSource " + child.tableName() + " has no hashedColumns");
        }
        return "SELECT " + child.fkColumn() + ", BIT_XOR(" + concatCrc32(hashed) + ") "
                + "FROM " + child.tableName()
                + " WHERE " + child.fkColumn() + " IN (" + placeholders(placeholderCount) + ") "
                + "GROUP BY " + child.fkColumn();
    }

    private static String placeholders(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("placeholderCount must be > 0, was " + count);
        }
        StringBuilder sb = new StringBuilder(count * 3);
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
        return sb.toString();
    }

    private static String concatCrc32(List<String> hashedColumns) {
        StringBuilder concat = new StringBuilder();
        concat.append("CRC32(CONCAT_WS(',', ");
        for (int i = 0; i < hashedColumns.size(); i++) {
            if (i > 0) concat.append(", ");
            concat.append(hashedColumns.get(i));
        }
        concat.append("))");
        return concat.toString();
    }
}
