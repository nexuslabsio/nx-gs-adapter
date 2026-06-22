package app.l2nx.gs.db.sync.engine.phase;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class ConsistentSnapshotTxnTest {

    @Test
    void runReadOnly_shouldPassSameConnectionToAction_andCommitOnSuccess() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);

        Connection observed = ConsistentSnapshotTxn.runReadOnly(conn, txnConn -> txnConn);

        assertSame(conn, observed, "single consistent-snapshot txn must hand the txn-bound Connection to the action");
        verify(conn).setAutoCommit(false);
        verify(init).execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
        verify(conn).commit();
        verify(conn).setAutoCommit(true);
        verify(conn, never()).rollback();
    }

    @Test
    void runReadOnly_shouldRollback_whenActionThrowsSqlException() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);

        SQLException original = new SQLException("phase-1 failed");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> ConsistentSnapshotTxn.runReadOnly(conn, c -> {
                    throw original;
                }));
        assertSame(original, thrown);
        verify(conn).rollback();
        verify(conn, never()).commit();
    }

    @Test
    void runReadOnly_shouldRollback_whenActionThrowsRuntimeException() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);

        RuntimeException original = new IllegalStateException("bug");
        RuntimeException thrown = assertThrows(
                IllegalStateException.class,
                () -> ConsistentSnapshotTxn.runReadOnly(conn, c -> {
                    throw original;
                }));
        assertSame(original, thrown);
        verify(conn).rollback();
        verify(conn, never()).commit();
    }

    @Test
    void runReadOnly_shouldRollback_whenActionThrowsError() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);

        Error original = new AssertionError("boom");
        Error thrown = assertThrows(
                AssertionError.class,
                () -> ConsistentSnapshotTxn.runReadOnly(conn, c -> {
                    throw original;
                }));
        assertSame(original, thrown);
        verify(conn).rollback();
    }

    @Test
    void runReadOnly_shouldRestoreAutoCommit_evenIfActionThrows() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement init = mock(Statement.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(init);

        assertThrows(
                SQLException.class,
                () -> ConsistentSnapshotTxn.runReadOnly(conn, c -> {
                    throw new SQLException("err");
                }));
        verify(conn).setAutoCommit(true);
    }
}
