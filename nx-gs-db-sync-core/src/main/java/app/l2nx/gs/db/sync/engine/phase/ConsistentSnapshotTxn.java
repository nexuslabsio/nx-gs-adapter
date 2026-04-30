package app.l2nx.gs.db.sync.engine.phase;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Wraps an SQL action in {@code START TRANSACTION WITH CONSISTENT SNAPSHOT,
 * READ ONLY}. Saves and restores the connection's prior autoCommit; rolls
 * back on any {@link SQLException} thrown by the action and rethrows;
 * commits otherwise.
 *
 * <p>Used by {@link Phase1Hasher} (per-window CRC scan) and
 * {@link Phase2Fetcher} (per-chunk row fetch). Per-query snapshots are an
 * explicit cdc-engine design decision (see spec.md Decisions): each phase /
 * each child source / each chunk runs its own short transaction so the
 * host DB never holds a multi-minute undo log on the adapter's behalf.</p>
 */
final class ConsistentSnapshotTxn {

    private ConsistentSnapshotTxn() {
    }

    static <T> T runReadOnly(Connection conn, SqlAction<T> action) throws SQLException {
        boolean priorAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            try (Statement init = conn.createStatement()) {
                init.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
            }
            T result = action.run();
            conn.commit();
            return result;
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
            throw e;
        } finally {
            try {
                conn.setAutoCommit(priorAutoCommit);
            } catch (SQLException ignored) {
            }
        }
    }

    @FunctionalInterface
    interface SqlAction<T> {
        T run() throws SQLException;
    }
}
