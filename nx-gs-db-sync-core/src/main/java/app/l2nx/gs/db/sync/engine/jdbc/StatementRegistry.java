package app.l2nx.gs.db.sync.engine.jdbc;

import java.sql.Statement;
import org.jspecify.annotations.Nullable;

/** Tracks the executing {@link Statement} so shutdown can cancel it instead of waiting for the driver socket timeout. */
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
                // Cancellation support varies by driver.
            }
        }
    }
}
