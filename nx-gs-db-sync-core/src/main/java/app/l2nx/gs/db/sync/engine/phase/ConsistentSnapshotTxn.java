package app.l2nx.gs.db.sync.engine.phase;

import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Wraps an SQL action in {@code START TRANSACTION WITH CONSISTENT SNAPSHOT,
 * READ ONLY}. Saves and restores the connection's prior autoCommit; rolls
 * back on any {@link Throwable} from the action and rethrows the original;
 * commits otherwise.
 */
public final class ConsistentSnapshotTxn {

    private static final NxLog log = NxLogFactory.getLogger(ConsistentSnapshotTxn.class);

    private ConsistentSnapshotTxn() {
    }

    public static <T> T runReadOnly(Connection conn, SqlAction<T> action) throws SQLException {
        return runReadOnly(conn, ignored -> action.run());
    }

    public static <T> T runReadOnly(Connection conn, ConnectionAction<T> action) throws SQLException {
        boolean priorAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            try (Statement init = conn.createStatement()) {
                init.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
            }
            T result = action.run(conn);
            conn.commit();
            return result;
        } catch (Throwable t) {
            try {
                conn.rollback();
            } catch (Exception rollbackError) {
                // Swallow rollback failure to preserve the original exception's type and cause chain.
            }
            if (t instanceof SQLException) {
                throw (SQLException) t;
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            // Checked non-SQLException — wrap to satisfy the signature.
            throw new SQLException(t);
        } finally {
            try {
                conn.setAutoCommit(priorAutoCommit);
            } catch (Throwable restoreError) {
                log.warn("Failed to restore autoCommit={} after consistent-snapshot txn: {}",
                        priorAutoCommit, restoreError);
            }
        }
    }

    @FunctionalInterface
    public interface SqlAction<T> {
        T run() throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionAction<T> {
        T run(Connection conn) throws SQLException;
    }
}
