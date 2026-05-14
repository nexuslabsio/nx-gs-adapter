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
 * Plans a per-cycle list of PK windows for one entity. Runs MIN/MAX over the
 * primary table once, unions with the snapshot's PK envelope so a deletion of
 * the current MIN or MAX still falls inside some next-cycle window, then
 * ceil-divides into chunks of {@code <= rowsPerWindow} width.
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
