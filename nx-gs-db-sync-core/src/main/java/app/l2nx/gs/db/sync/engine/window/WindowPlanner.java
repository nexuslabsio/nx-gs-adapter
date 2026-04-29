package app.l2nx.gs.db.sync.engine.window;

import app.l2nx.gs.adapter.api.spi.EntityMapping;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Plans a per-cycle list of PK windows for one entity by running
 * {@code SELECT MIN(pk), MAX(pk) FROM table} once and ceil-dividing the resulting
 * closed range {@code [MIN, MAX]} into chunks of {@code <= rowsPerWindow}
 * width.
 *
 * <p>Degenerate cases:</p>
 * <ul>
 *     <li>Empty table (MIN/MAX both NULL) — returns empty list. Engine treats
 *     as "no windows to scan this cycle".</li>
 *     <li>Range fits in one window ({@code MAX - MIN + 1 <= rowsPerWindow}) —
 *     returns a single-window list.</li>
 *     <li>Single row ({@code MIN == MAX}) — single window {@code [pk, pk]}.</li>
 * </ul>
 *
 * <p>Window boundaries are recomputed at every cycle so PK growth (new rows
 * arriving with id > current MAX) is naturally absorbed by the next cycle's
 * plan. There is no caching across cycles.</p>
 */
public final class WindowPlanner {

    public List<Window> plan(EntityMapping<?> mapping,
                             Connection conn,
                             int rowsPerWindow,
                             int queryTimeoutSeconds) throws SQLException {
        if (rowsPerWindow <= 0) {
            throw new IllegalArgumentException(
                    "rowsPerWindow must be > 0, was " + rowsPerWindow);
        }
        String sql = "SELECT MIN(" + mapping.pkColumn() + "), MAX(" + mapping.pkColumn() + ") "
                + "FROM " + mapping.tableName();
        long minPk;
        long maxPk;
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = st.executeQuery(sql)) {
                if (!rs.next()) {
                    return Collections.emptyList();
                }
                minPk = rs.getLong(1);
                boolean minWasNull = rs.wasNull();
                maxPk = rs.getLong(2);
                boolean maxWasNull = rs.wasNull();
                if (minWasNull || maxWasNull) {
                    return Collections.emptyList();
                }
            }
        }
        return divideRange(minPk, maxPk, rowsPerWindow);
    }

    /**
     * Hard cap on plan size — sanity guard against an overflow-induced explosion
     * when MIN/MAX span the full BIGINT range and {@code rowsPerWindow} is small.
     * Real schemas never approach this; hitting the cap means the underlying data
     * is either pathological (PK growth gone wrong) or {@code rowsPerWindow} is
     * misconfigured. Engine logs a warn and treats the entity as DEGRADED rather
     * than OOM-ing the host JVM.
     */
    static final int MAX_WINDOWS_PER_PLAN = 1_000_000;

    static List<Window> divideRange(long minPk, long maxPk, int rowsPerWindow) {
        if (maxPk < minPk) {
            return Collections.emptyList();
        }
        // span = maxPk - minPk + 1 overflows when the closed range exceeds Long.MAX_VALUE.
        // Detect via the unsigned subtraction: a negative result means the span doesn't fit
        // in a long, which forces chunking regardless of rowsPerWindow.
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
                throw new IllegalStateException(
                        "WindowPlanner produced > " + MAX_WINDOWS_PER_PLAN
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
