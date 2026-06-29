package app.l2nx.gs.db.sync.engine.window;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A PK window for one Phase-1 hash query. Two shapes:
 *
 * <ul>
 *   <li><b>Range</b> — closed interval {@code [fromPk, toPk]} matching
 *   {@code WHERE pk BETWEEN ? AND ?}. Produced by {@link WindowPlanner#plan},
 *   used by full scheduled / whole-entity cycles.</li>
 *   <li><b>Targeted</b> — an explicit PK list matching
 *   {@code WHERE pk IN (?, ?, ...)}. Produced by
 *   {@link WindowPlanner#planTargeted}, used by the per-PK force-resync
 *   fast-path so a triggered cycle hashes only the invalidated rows instead of
 *   full-scanning the table. {@code fromPk} / {@code toPk} are the min / max of
 *   the PK list (so snapshot bucketing still works), but the WHERE clause is an
 *   {@code IN}-list, not a range.</li>
 * </ul>
 *
 * <p>Consumed by {@link app.l2nx.gs.db.sync.engine.phase.Phase1Hasher}.</p>
 */
public final class Window {

    private final long fromPk;
    private final long toPk;
    private final @Nullable LongList pks;

    public Window(long fromPk, long toPk) {
        if (toPk < fromPk) {
            throw new IllegalArgumentException("Window toPk=" + toPk + " < fromPk=" + fromPk);
        }
        this.fromPk = fromPk;
        this.toPk = toPk;
        this.pks = null;
    }

    private Window(long fromPk, long toPk, LongList pks) {
        this.fromPk = fromPk;
        this.toPk = toPk;
        this.pks = pks;
    }

    /**
     * Builds a targeted {@code IN}-list window from an explicit, non-empty PK
     * list. The list is copied defensively; {@code fromPk} / {@code toPk} are
     * set to its min / max so snapshot bucketing locates the window.
     */
    public static Window ofPks(LongList pks) {
        if (pks == null || pks.isEmpty()) {
            throw new IllegalArgumentException("targeted Window requires a non-empty PK list");
        }
        LongArrayList copy = new LongArrayList(pks);
        long min = copy.getLong(0);
        long max = copy.getLong(0);
        for (int i = 1; i < copy.size(); i++) {
            long pk = copy.getLong(i);
            if (pk < min) min = pk;
            if (pk > max) max = pk;
        }
        return new Window(min, max, copy);
    }

    public long fromPk() {
        return fromPk;
    }

    public long toPk() {
        return toPk;
    }

    /**
     * {@code true} when this is a targeted {@code IN}-list window;
     * {@code false} for a range window.
     */
    public boolean targeted() {
        return pks != null;
    }

    /**
     * The explicit PK list of a targeted window. {@code null} for a range
     * window — callers must guard with {@link #targeted()}.
     */
    public @Nullable LongList pks() {
        return pks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Window)) return false;
        Window w = (Window) o;
        return fromPk == w.fromPk && toPk == w.toPk && Objects.equals(pks, w.pks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromPk, toPk, pks);
    }

    @Override
    public String toString() {
        if (pks != null) {
            return "Window[IN " + pks + "]";
        }
        return "Window[" + fromPk + ", " + toPk + "]";
    }
}
