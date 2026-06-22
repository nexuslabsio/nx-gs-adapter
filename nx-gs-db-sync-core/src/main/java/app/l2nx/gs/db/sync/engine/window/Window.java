package app.l2nx.gs.db.sync.engine.window;

import java.util.Objects;

/**
 * Closed-interval PK window {@code [fromPk, toPk]} matching MySQL
 * {@code WHERE pk BETWEEN ? AND ?} semantics. Produced by {@link WindowPlanner};
 * consumed by {@link app.l2nx.gs.db.sync.engine.phase.Phase1Hasher}.
 */
public final class Window {

    private final long fromPk;
    private final long toPk;

    public Window(long fromPk, long toPk) {
        if (toPk < fromPk) {
            throw new IllegalArgumentException("Window toPk=" + toPk + " < fromPk=" + fromPk);
        }
        this.fromPk = fromPk;
        this.toPk = toPk;
    }

    public long fromPk() {
        return fromPk;
    }

    public long toPk() {
        return toPk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Window)) return false;
        Window w = (Window) o;
        return fromPk == w.fromPk && toPk == w.toPk;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromPk, toPk);
    }

    @Override
    public String toString() {
        return "Window[" + fromPk + ", " + toPk + "]";
    }
}
