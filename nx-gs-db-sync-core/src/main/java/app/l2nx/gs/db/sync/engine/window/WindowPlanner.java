package app.l2nx.gs.db.sync.engine.window;

import app.l2nx.gs.adapter.api.spi.model.EntityMapping;
import app.l2nx.gs.adapter.api.spi.model.PrimarySource;
import app.l2nx.gs.db.sync.engine.SnapshotStore;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Plans per-cycle PK windows: MIN/MAX of the primary table unioned with the snapshot's PK envelope (so a deleted MIN/MAX still
 * falls in some window), ceil-divided into chunks of at most {@code rowsPerWindow}.
 */
public final class WindowPlanner {

    public List<Window> plan(
            EntityMapping<?> mapping,
            Connection conn,
            SnapshotStore snapshot,
            int rowsPerWindow,
            int queryTimeoutSeconds)
            throws SQLException {
        if (rowsPerWindow <= 0) {
            throw new IllegalArgumentException("rowsPerWindow must be > 0, was " + rowsPerWindow);
        }
        PrimarySource<?> primary = mapping.primary();
        OptionalLong minDb = OptionalLong.empty();
        OptionalLong maxDb = OptionalLong.empty();

        String sql = "SELECT MIN(" + primary.pkColumn() + "), MAX(" + primary.pkColumn() + ") " + "FROM "
                + primary.tableName();
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    long min = rs.getLong(1);
                    boolean minWasNull = rs.wasNull();
                    long max = rs.getLong(2);
                    boolean maxWasNull = rs.wasNull();
                    if (!minWasNull && !maxWasNull) {
                        minDb = OptionalLong.of(min);
                        maxDb = OptionalLong.of(max);
                    }
                }
            }
        }

        OptionalLong minSnap = snapshot.minPk(mapping.entityName());
        OptionalLong maxSnap = snapshot.maxPk(mapping.entityName());

        OptionalLong minEnv = unionMin(minDb, minSnap);
        OptionalLong maxEnv = unionMax(maxDb, maxSnap);
        if (!minEnv.isPresent() || !maxEnv.isPresent()) {
            return Collections.emptyList();
        }
        return divideRange(minEnv.getAsLong(), maxEnv.getAsLong(), rowsPerWindow);
    }

    /** Chunks the invalidated PKs into bounded {@code IN}-list windows; empty for null/empty. {@code conn} is unused (signature symmetry with {@link #plan}). */
    public List<Window> planTargeted(EntityMapping<?> mapping, Connection conn, LongSet targetedPks) {
        if (targetedPks == null || targetedPks.isEmpty()) {
            return Collections.emptyList();
        }
        LongList all = Phase2Fetcher.toList(targetedPks);
        List<Window> windows = new ArrayList<Window>();
        int total = all.size();
        int from = 0;
        while (from < total) {
            int to = Math.min(from + TARGETED_CHUNK_SIZE, total);
            LongArrayList chunk = new LongArrayList(to - from);
            for (int i = from; i < to; i++) {
                chunk.add(all.getLong(i));
            }
            windows.add(Window.ofPks(chunk));
            from = to;
        }
        return windows;
    }

    /** Reuses the cascade resolution chunk size to keep the same bounded {@code IN(...)} cardinality. */
    public static final int TARGETED_CHUNK_SIZE = 500;

    private static OptionalLong unionMin(OptionalLong a, OptionalLong b) {
        if (!a.isPresent()) return b;
        if (!b.isPresent()) return a;
        return OptionalLong.of(Math.min(a.getAsLong(), b.getAsLong()));
    }

    private static OptionalLong unionMax(OptionalLong a, OptionalLong b) {
        if (!a.isPresent()) return b;
        if (!b.isPresent()) return a;
        return OptionalLong.of(Math.max(a.getAsLong(), b.getAsLong()));
    }

    /** Sanity cap against overflow-induced plan explosion (MIN/MAX spanning BIGINT with small {@code rowsPerWindow}); hitting it makes the entity DEGRADED instead of OOM. */
    static final int MAX_WINDOWS_PER_PLAN = 1_000_000;

    static List<Window> divideRange(long minPk, long maxPk, int rowsPerWindow) {
        if (maxPk < minPk) {
            return Collections.emptyList();
        }
        // The span overflows a long when the closed range exceeds Long.MAX_VALUE; a negative unsigned subtraction
        // forces chunking.
        long rawSpan = maxPk - minPk;
        boolean spanFitsInLong = rawSpan >= 0L && rawSpan < Long.MAX_VALUE;
        if (spanFitsInLong && (rawSpan + 1L) <= rowsPerWindow) {
            return Collections.singletonList(new Window(minPk, maxPk));
        }
        List<Window> windows = new ArrayList<Window>();
        long cursor = minPk;
        while (true) {
            long end = cursor + rowsPerWindow - 1L;
            if (end < cursor || end > maxPk) {
                end = maxPk;
            }
            windows.add(new Window(cursor, end));
            if (windows.size() > MAX_WINDOWS_PER_PLAN) {
                throw new IllegalStateException("WindowPlanner produced > " + MAX_WINDOWS_PER_PLAN
                        + " windows for [" + minPk + ", " + maxPk + "] with rowsPerWindow="
                        + rowsPerWindow + " — check PK range and rows-per-window config");
            }
            if (end == maxPk) {
                break;
            }
            cursor = end + 1L;
        }
        return windows;
    }
}
