package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.JdbcDialect;
import app.l2nx.gs.db.sync.engine.StatementRegistry;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 2: fetches row data for PKs whose aggregate CRC32 changed. PK lists
 * are chunked at {@value #CHUNK_SIZE}; the last (smaller) chunk pads to the
 * fixed placeholder count by repeating its final PK — IN(...) dedupes server-
 * side, the prepared-statement string stays cache-stable across chunks.
 *
 * <p>Phase-2 missing rows (present in Phase 1, gone now) are a silent no-op —
 * next cycle's Phase-1 catches the deletion.</p>
 */
public final class Phase2Fetcher {

    static final int CHUNK_SIZE = 1000;

    public Long2ObjectMap<Object> fetchPrimary(Connection conn,
                                               PrimarySource<?> primary,
                                               LongList pks,
                                               int queryTimeoutSeconds,
                                               int fetchSize,
                                               JdbcDialect dialect) throws SQLException {
        return fetchPrimary(conn, primary, pks, queryTimeoutSeconds, fetchSize, dialect, null);
    }

    public Long2ObjectMap<Object> fetchPrimary(Connection conn,
                                               PrimarySource<?> primary,
                                               LongList pks,
                                               int queryTimeoutSeconds,
                                               int fetchSize,
                                               JdbcDialect dialect,
                                               @Nullable StatementRegistry registry) throws SQLException {
        Long2ObjectOpenHashMap<Object> result = new Long2ObjectOpenHashMap<Object>();
        if (pks == null || pks.isEmpty()) {
            return result;
        }
        String sql = buildSql(primary.tableName(), primary.pkColumn(), CHUNK_SIZE);
        runChunkedFetch(conn, pks, queryTimeoutSeconds, fetchSize, dialect, sql, registry, new RowConsumer() {
            @Override
            public void accept(ResultSet rs) throws SQLException {
                long pk = rs.getLong(primary.pkColumn());
                Object row = primary.mapRow(rs);
                result.put(pk, row);
            }
        });
        return result;
    }

    public Long2ObjectMap<List<Object>> fetchChild(Connection conn,
                                                   ChildSource<?> child,
                                                   LongList fks,
                                                   int queryTimeoutSeconds,
                                                   int fetchSize,
                                                   JdbcDialect dialect) throws SQLException {
        return fetchChild(conn, child, fks, queryTimeoutSeconds, fetchSize, dialect, null);
    }

    public Long2ObjectMap<List<Object>> fetchChild(Connection conn,
                                                   ChildSource<?> child,
                                                   LongList fks,
                                                   int queryTimeoutSeconds,
                                                   int fetchSize,
                                                   JdbcDialect dialect,
                                                   @Nullable StatementRegistry registry) throws SQLException {
        Long2ObjectOpenHashMap<List<Object>> result = new Long2ObjectOpenHashMap<List<Object>>();
        if (fks == null || fks.isEmpty()) {
            return result;
        }
        String sql = buildSql(child.tableName(), child.fkColumn(), CHUNK_SIZE);
        runChunkedFetch(conn, fks, queryTimeoutSeconds, fetchSize, dialect, sql, registry, new RowConsumer() {
            @Override
            public void accept(ResultSet rs) throws SQLException {
                long fk = rs.getLong(child.fkColumn());
                Object row = child.mapRow(rs);
                List<Object> bucket = result.get(fk);
                if (bucket == null) {
                    // Presize for the common per-parent fanout.
                    bucket = new ArrayList<Object>(8);
                    result.put(fk, bucket);
                }
                bucket.add(row);
            }
        });
        return result;
    }

    private static void runChunkedFetch(Connection conn,
                                        LongList pks,
                                        int queryTimeoutSeconds,
                                        int fetchSize,
                                        JdbcDialect dialect,
                                        String sql,
                                        @Nullable StatementRegistry registry,
                                        RowConsumer rowConsumer) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (registry != null) {
                registry.set(ps);
            }
            try {
                ps.setQueryTimeout(queryTimeoutSeconds);
                Phase1Hasher.applyFetchSize(ps, fetchSize, dialect);
                int from = 0;
                int total = pks.size();
                while (from < total) {
                    int to = Math.min(from + CHUNK_SIZE, total);
                    fetchChunk(pks, from, to, ps, rowConsumer);
                    from = to;
                }
            } finally {
                if (registry != null) {
                    registry.clear();
                }
            }
        }
    }

    private static void fetchChunk(LongList pks,
                                   int from,
                                   int to,
                                   PreparedStatement ps,
                                   RowConsumer rowConsumer) throws SQLException {
        int count = to - from;
        long padPk = pks.getLong(to - 1);
        for (int i = 0; i < count; i++) {
            ps.setLong(i + 1, pks.getLong(from + i));
        }
        for (int i = count; i < CHUNK_SIZE; i++) {
            ps.setLong(i + 1, padPk);
        }
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                rowConsumer.accept(rs);
            }
        }
    }

    static String buildSql(String tableName, String keyColumn, int placeholderCount) {
        if (placeholderCount <= 0) {
            throw new IllegalArgumentException(
                    "placeholderCount must be > 0, was " + placeholderCount);
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(tableName)
                .append(" WHERE ").append(keyColumn).append(" IN (");
        for (int i = 0; i < placeholderCount; i++) {
            if (i > 0) sql.append(", ");
            sql.append('?');
        }
        sql.append(')');
        return sql.toString();
    }

    /**
     * Iterate-and-copy helper for callers that hold a primitive {@code LongSet}
     * but need a deterministic-ordered {@link LongList} for chunking.
     */
    public static LongList toList(it.unimi.dsi.fastutil.longs.LongSet keys) {
        it.unimi.dsi.fastutil.longs.LongArrayList list = new it.unimi.dsi.fastutil.longs.LongArrayList(keys.size());
        LongIterator it = keys.iterator();
        while (it.hasNext()) {
            list.add(it.nextLong());
        }
        return list;
    }

    private interface RowConsumer {
        void accept(ResultSet rs) throws SQLException;
    }
}
