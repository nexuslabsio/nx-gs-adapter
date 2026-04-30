package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Phase 1 of the CRC32 CDC protocol. Two windowed-query variants:
 *
 * <ul>
 *     <li>{@link #hashPrimary} — {@code SELECT pk, CRC32(CONCAT_WS(',',
 *     col1, col2, ...)) FROM primary WHERE pk BETWEEN ? AND ?}. Returns a
 *     {@code Long2IntMap} of (PK → primary CRC32).</li>
 *     <li>{@link #hashChild} — {@code SELECT fk,
 *     BIT_XOR(CRC32(CONCAT_WS(',', col1, col2, ...))) FROM child WHERE fk
 *     BETWEEN ? AND ? GROUP BY fk}. Returns (parent PK → XOR-aggregate of
 *     every child row's CRC32 belonging to that parent). The XOR aggregate
 *     is order-insensitive, so child row order in the source has no
 *     bearing on the hash; engine XOR-folds the result into the per-PK
 *     aggregate built from primary + every other child.</li>
 * </ul>
 *
 * <p>Each scan runs inside an InnoDB consistent snapshot
 * ({@code START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY}) so the
 * window observes a coherent view even if writes land mid-scan. The
 * transaction commits as soon as the result is drained — no locks held
 * into the next window.</p>
 *
 * <p>{@code SET TRANSACTION READ ONLY} is set explicitly via the
 * {@link Connection#setReadOnly(boolean) setReadOnly(true)} hint applied by
 * the engine before borrow. The {@code WITH CONSISTENT SNAPSHOT} keyword
 * ensures the snapshot is established at transaction start, not on first
 * read, eliminating phantom anomalies between MIN/MAX planning and actual
 * scan.</p>
 *
 * <p>Collision risk for child {@code BIT_XOR(CRC32(...))} aggregates: two
 * child rows with identical CRC32 inside the same FK group cancel out in
 * XOR. Probability per pair is {@code ~1/2^32}; per-cycle change-miss
 * probability for an entity with N child rows is bounded by
 * {@code N(N-1)/2 × 1/2^32}. Acceptable for game-data eventual
 * consistency; a row-count guard ({@code XOR COUNT(*)}) is documented in
 * tech.md Decisions but deferred until a real collision surfaces.</p>
 */
public final class Phase1Hasher {

    public Long2IntMap hashPrimary(Window window,
                                   PrimarySource<?> primary,
                                   Connection conn,
                                   int queryTimeoutSeconds) throws SQLException {
        String sql = buildPrimarySql(primary);
        return runHashQuery(window, sql, conn, queryTimeoutSeconds);
    }

    public Long2IntMap hashChild(Window window,
                                 ChildSource<?> child,
                                 Connection conn,
                                 int queryTimeoutSeconds) throws SQLException {
        String sql = buildChildSql(child);
        return runHashQuery(window, sql, conn, queryTimeoutSeconds);
    }

    private static Long2IntMap runHashQuery(Window window,
                                            String sql,
                                            Connection conn,
                                            int queryTimeoutSeconds) throws SQLException {
        return ConsistentSnapshotTxn.runReadOnly(conn, () -> {
            Long2IntOpenHashMap result = new Long2IntOpenHashMap();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                ps.setLong(1, window.fromPk());
                ps.setLong(2, window.toPk());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long pk = rs.getLong(1);
                        // CRC32 / BIT_XOR(CRC32) in MySQL return BIGINT UNSIGNED
                        // (0..2^32-1); narrow to int — same bytes, comparison is
                        // bit-exact.
                        int crc = (int) rs.getLong(2);
                        result.put(pk, crc);
                    }
                }
            }
            return result;
        });
    }

    static String buildPrimarySql(PrimarySource<?> primary) {
        List<String> hashed = primary.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException(
                    "PrimarySource " + primary.tableName() + " has no hashedColumns");
        }
        return "SELECT " + primary.pkColumn() + ", " + concatCrc32(hashed)
                + " FROM " + primary.tableName()
                + " WHERE " + primary.pkColumn() + " BETWEEN ? AND ?";
    }

    static String buildChildSql(ChildSource<?> child) {
        List<String> hashed = child.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException(
                    "ChildSource " + child.tableName() + " has no hashedColumns");
        }
        return "SELECT " + child.fkColumn() + ", BIT_XOR(" + concatCrc32(hashed) + ") "
                + "FROM " + child.tableName()
                + " WHERE " + child.fkColumn() + " BETWEEN ? AND ? "
                + "GROUP BY " + child.fkColumn();
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
