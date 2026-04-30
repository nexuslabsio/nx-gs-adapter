package app.l2nx.gs.db.sync.engine.window;

import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import app.l2nx.gs.db.sync.engine.SnapshotStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Plans a per-cycle list of PK windows for one entity. Runs
 * {@code SELECT MIN(pk), MAX(pk) FROM <primary.tableName>} once, reads
 * {@link SnapshotStore#minPk}/{@link SnapshotStore#maxPk} for the same entity,
 * computes the envelope {@code [min(MIN_db, MIN_snap), max(MAX_db, MAX_snap)]}
 * and ceil-divides it into chunks of {@code <= rowsPerWindow} width.
 *
 * <p>Envelope rationale (cdc-engine R2): when the row at the current
 * {@code MIN(pk)} or {@code MAX(pk)} is deleted, the DB range shrinks below
 * (or above) the deleted PK; partitioning the shrunken range alone leaves
 * the deleted PK outside every next-cycle window and its tombstone never
 * fires. Including the snapshot's PK extremes in the envelope keeps every
 * PK ever seen in scope until its tombstone is published and its CRC removed
 * — converging back to {@code [MIN_db, MAX_db]} once the drift drains.</p>
 *
 * <p>Degenerate cases:</p>
 * <ul>
 *     <li>Both DB and snapshot empty (cold start + empty primary table) —
 *     returns empty list. Engine treats as "no work this cycle".</li>
 *     <li>One side empty — the other drives the envelope (initial cycle uses
 *     {@code [MIN_db, MAX_db]}; a fully-drained DB with leftover snapshot
 *     uses {@code [MIN_snap, MAX_snap]} so all leftover PKs get tombstone
 *     events on the next cycle).</li>
 *     <li>Range fits in one window
 *     ({@code maxEnv - minEnv + 1 <= rowsPerWindow}) — single-window list.</li>
 *     <li>Single PK ({@code minEnv == maxEnv}) — single window
 *     {@code [pk, pk]}.</li>
 * </ul>
 *
 * <p>Window boundaries are recomputed at every cycle so PK growth is
 * naturally absorbed; there is no caching across cycles.</p>
 */
public final class WindowPlanner {

    public List<Window> plan(EntityMapping<?> mapping,
                             Connection conn,
                             SnapshotStore snapshot,
                             int rowsPerWindow,
                             int queryTimeoutSeconds) throws SQLException {
        if (rowsPerWindow <= 0) {
            throw new IllegalArgumentException(
                    "rowsPerWindow must be > 0, was " + rowsPerWindow);
        }
        PrimarySource<?> primary = mapping.primary();
        OptionalLong minDb = OptionalLong.empty();
        OptionalLong maxDb = OptionalLong.empty();

        String sql = "SELECT MIN(" + primary.pkColumn() + "), MAX(" + primary.pkColumn() + ") "
                + "FROM " + primary.tableName();
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
