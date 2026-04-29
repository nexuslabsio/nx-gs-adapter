package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Phase 1 of the CRC32 CDC protocol: scans one window of one entity, returns
 * a {@code PK → CRC32(concatenated columns)} map. CRC32 is computed server-side
 * in MySQL via {@code CRC32(CONCAT_WS(',', col1, col2, ...))} so the adapter
 * pulls only one int per row instead of the full row payload — Phase 2 fetches
 * the actual row data only for PKs whose CRC32 changed.
 *
 * <p>Each scan runs inside an InnoDB consistent snapshot
 * ({@code START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY}) so the
 * window observes a coherent view even if writes land mid-scan. The transaction
 * commits as soon as the result is drained — no locks held into the next
 * window.</p>
 *
 * <p>{@code SET TRANSACTION READ ONLY} is set explicitly via the
 * {@link Connection#setReadOnly(boolean) setReadOnly(true)} hint applied by
 * the engine before borrow. The {@code WITH CONSISTENT SNAPSHOT} keyword
 * ensures the snapshot is established at transaction start, not on first read,
 * eliminating phantom anomalies between MIN/MAX planning and actual scan.</p>
 */
public final class Phase1Hasher {

    public Long2IntMap hash(Window window,
                            EntityMapping<?> mapping,
                            Connection conn,
                            int queryTimeoutSeconds) throws SQLException {
        String sql = buildSql(mapping);
        Long2IntOpenHashMap result = new Long2IntOpenHashMap();
        boolean priorAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            try (java.sql.Statement init = conn.createStatement()) {
                init.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
            }
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                ps.setLong(1, window.fromPk());
                ps.setLong(2, window.toPk());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long pk = rs.getLong(1);
                        // CRC32 in MySQL returns BIGINT UNSIGNED (0..2^32-1); narrow to int.
                        int crc = (int) rs.getLong(2);
                        result.put(pk, crc);
                    }
                }
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
            throw e;
        } finally {
            try {
                conn.setAutoCommit(priorAutoCommit);
            } catch (SQLException ignored) {
            }
        }
        return result;
    }

    static String buildSql(EntityMapping<?> mapping) {
        List<String> hashed = mapping.hashedColumns();
        if (hashed == null || hashed.isEmpty()) {
            throw new IllegalArgumentException(
                    "EntityMapping " + mapping.entityName() + " has no hashedColumns");
        }
        StringBuilder concat = new StringBuilder();
        concat.append("CRC32(CONCAT_WS(',', ");
        for (int i = 0; i < hashed.size(); i++) {
            if (i > 0) concat.append(", ");
            concat.append(hashed.get(i));
        }
        concat.append("))");
        return "SELECT " + mapping.pkColumn() + ", " + concat
                + " FROM " + mapping.tableName()
                + " WHERE " + mapping.pkColumn() + " BETWEEN ? AND ?";
    }
}
