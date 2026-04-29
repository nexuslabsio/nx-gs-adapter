package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Phase 2 of the CRC32 CDC protocol: fetches the actual row data for the PKs
 * whose CRC32 changed (created ∪ updated). DELETE PKs are NOT fetched —
 * the engine emits a Kafka tombstone for them with {@code payload=null}.
 *
 * <p>PK lists are chunked at {@value #CHUNK_SIZE} entries to keep
 * {@code IN(...)} clauses below MySQL's max-prepared-stmt-arg limits and
 * to bound prepare-cache pressure (one cached plan per chunk size).</p>
 *
 * <p>Phase-2 missing rows (a PK that was present in Phase 1 but disappeared
 * by the time Phase 2 runs) are a silent no-op per the resolved decision —
 * the next cycle's Phase-1 diff catches the deletion as a true DELETED and
 * emits the tombstone then. No fabricated DELETED in the same cycle.</p>
 */
public final class Phase2Fetcher {

    static final int CHUNK_SIZE = 1000;

    public <T> Long2ObjectMap<T> fetch(EntityMapping<T> mapping,
                                       LongList pks,
                                       Connection conn,
                                       int queryTimeoutSeconds) throws SQLException {
        Long2ObjectOpenHashMap<T> result = new Long2ObjectOpenHashMap<T>();
        if (pks == null || pks.isEmpty()) {
            return result;
        }
        boolean priorAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            try (java.sql.Statement init = conn.createStatement()) {
                init.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
            }
            // Single PreparedStatement with fixed CHUNK_SIZE placeholders. Last (smaller)
            // chunk pads by repeating its final PK — IN-set semantics dedupe, so the
            // result is identical, but the SQL string stays constant across all chunks
            // in this call → server-side prepared-statement cache hits every time.
            String sql = buildSql(mapping, CHUNK_SIZE);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                int from = 0;
                int total = pks.size();
                while (from < total) {
                    int to = Math.min(from + CHUNK_SIZE, total);
                    fetchChunk(mapping, pks, from, to, ps, result);
                    from = to;
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

    private <T> void fetchChunk(EntityMapping<T> mapping,
                                LongList pks,
                                int from,
                                int to,
                                PreparedStatement ps,
                                Long2ObjectOpenHashMap<T> result) throws SQLException {
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
                long pk = rs.getLong(mapping.pkColumn());
                T dto = mapping.mapRow(rs);
                result.put(pk, dto);
            }
        }
    }

    static String buildSql(EntityMapping<?> mapping, int placeholderCount) {
        if (placeholderCount <= 0) {
            throw new IllegalArgumentException(
                    "placeholderCount must be > 0, was " + placeholderCount);
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(mapping.tableName())
                .append(" WHERE ").append(mapping.pkColumn()).append(" IN (");
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
}
