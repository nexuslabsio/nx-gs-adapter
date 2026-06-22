package app.l2nx.gs.db.sync.engine;

import java.sql.Statement;
import org.jspecify.annotations.Nullable;

/**
 * Tracks the JDBC {@link Statement} currently executing for a sync task so
 * the engine can {@link Statement#cancel()} it on shutdown. Without this,
 * a hung query inside Phase-1 / Phase-2 keeps the daemon thread blocked
 * until the driver-side socket timeout fires.
 */
public final class StatementRegistry {

    private volatile @Nullable Statement current;

    public void set(Statement statement) {
        this.current = statement;
    }

    public void clear() {
        this.current = null;
    }

    public @Nullable Statement current() {
        return current;
    }

    public void cancelCurrent() {
        Statement s = current;
        if (s != null) {
            try {
                s.cancel();
            } catch (Throwable ignore) {
                // Best-effort — JDBC drivers vary in cancellation support.
            }
        }
    }
}
