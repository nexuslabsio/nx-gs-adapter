package app.l2nx.gs.db.sync.engine.window;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A PK window for one Phase-1 query: either a closed range ({@code BETWEEN}, from {@link WindowPlanner#plan}) or a targeted
 * {@code IN}-list ({@link WindowPlanner#planTargeted}); a targeted window's {@code fromPk}/{@code toPk} are the list's min/max for snapshot bucketing.
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

    /** Copies the non-empty list defensively; {@code fromPk}/{@code toPk} are its min/max. */
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

    public boolean targeted() {
        return pks != null;
    }

    /** {@code null} for a range window; guard with {@link #targeted()}. */
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
