package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 2 of the CRC32 CDC protocol: fetches the actual row data for the PKs
 * whose aggregate CRC32 changed (created ∪ updated). Two variants:
 *
 * <ul>
 *     <li>{@link #fetchPrimary} — {@code SELECT * FROM primary WHERE pk IN
 *     (?, ?, ...)} chunked at {@value #CHUNK_SIZE}. Returns {@code Long2ObjectMap<Object>}
 *     (PK → opaque row produced by {@code primary.mapRow}).</li>
 *     <li>{@link #fetchChild} — {@code SELECT * FROM child WHERE fk IN (?, ?,
 *     ...)} chunked at {@value #CHUNK_SIZE}; rows grouped by FK. Returns
 *     {@code Long2ObjectMap<List<Object>>} (FK → list of opaque rows). FK
 *     absence means "no children for this PK" — caller substitutes an empty
 *     list when calling {@code mapEntity}.</li>
 * </ul>
 *
 * <p>DELETE PKs are NOT fetched — the engine emits a Kafka tombstone for them
 * with {@code payload=null}.</p>
 *
 * <p>PK lists are chunked at {@value #CHUNK_SIZE} entries to keep
 * {@code IN(...)} clauses below MySQL's max-prepared-stmt-arg limits and
 * to bound prepare-cache pressure (one cached plan per chunk size). The last
 * (smaller) chunk pads by repeating its final PK so the SQL string stays
 * stable across chunks (server-side prepared-statement cache hits).</p>
 *
 * <p>Phase-2 missing rows (a PK that was present in Phase 1 but disappeared
 * by the time Phase 2 runs) are a silent no-op per the resolved decision —
 * the next cycle's Phase-1 diff catches the deletion as a true DELETED and
 * emits the tombstone then. No fabricated DELETED in the same cycle.</p>
 */
public final class Phase2Fetcher {

    static final int CHUNK_SIZE = 1000;

    public Long2ObjectMap<Object> fetchPrimary(PrimarySource<?> primary,
                                               LongList pks,
                                               Connection conn,
                                               int queryTimeoutSeconds) throws SQLException {
        Long2ObjectOpenHashMap<Object> result = new Long2ObjectOpenHashMap<Object>();
        if (pks == null || pks.isEmpty()) {
            return result;
        }
        String sql = buildSql(primary.tableName(), primary.pkColumn(), CHUNK_SIZE);
        runChunkedFetch(pks, conn, queryTimeoutSeconds, sql, new RowConsumer() {
            @Override
            public void accept(ResultSet rs) throws SQLException {
                long pk = rs.getLong(primary.pkColumn());
                Object row = primary.mapRow(rs);
                result.put(pk, row);
            }
        });
        return result;
    }

    public Long2ObjectMap<List<Object>> fetchChild(ChildSource<?> child,
                                                   LongList fks,
                                                   Connection conn,
                                                   int queryTimeoutSeconds) throws SQLException {
        Long2ObjectOpenHashMap<List<Object>> result = new Long2ObjectOpenHashMap<List<Object>>();
        if (fks == null || fks.isEmpty()) {
            return result;
        }
        String sql = buildSql(child.tableName(), child.fkColumn(), CHUNK_SIZE);
        runChunkedFetch(fks, conn, queryTimeoutSeconds, sql, new RowConsumer() {
            @Override
            public void accept(ResultSet rs) throws SQLException {
                long fk = rs.getLong(child.fkColumn());
                Object row = child.mapRow(rs);
                List<Object> bucket = result.get(fk);
                if (bucket == null) {
                    bucket = new ArrayList<Object>();
                    result.put(fk, bucket);
                }
                bucket.add(row);
            }
        });
        return result;
    }

    private static void runChunkedFetch(LongList pks,
                                        Connection conn,
                                        int queryTimeoutSeconds,
                                        String sql,
                                        RowConsumer rowConsumer) throws SQLException {
        // Single PreparedStatement with fixed CHUNK_SIZE placeholders. Last (smaller)
        // chunk pads by repeating its final PK — IN-set semantics dedupe, so the
        // result is identical, but the SQL string stays constant across all chunks
        // in this call → server-side prepared-statement cache hits every time.
        ConsistentSnapshotTxn.runReadOnly(conn, () -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                int from = 0;
                int total = pks.size();
                while (from < total) {
                    int to = Math.min(from + CHUNK_SIZE, total);
                    fetchChunk(pks, from, to, ps, rowConsumer);
                    from = to;
                }
            }
            return null;
        });
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
        // Pad to CHUNK_SIZE by repeating the last real PK — duplicates in IN(...) are deduped server-side.
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
