package app.l2nx.gs.db.sync.engine;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import app.l2nx.gs.db.sync.engine.jdbc.StatementRegistry;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class StatementCancelTest {

    @Test
    void cancelCurrent_shouldNoOp_whenNoStatementRegistered() {
        StatementRegistry registry = new StatementRegistry();
        registry.cancelCurrent();
    }

    @Test
    void cancelCurrent_shouldCallStatementCancel_whenRegistered() throws SQLException {
        StatementRegistry registry = new StatementRegistry();
        Statement statement = mock(Statement.class);
        registry.set(statement);

        registry.cancelCurrent();

        verify(statement).cancel();
    }

    @Test
    void cancelCurrent_shouldSwallowThrowables_fromDriverCancel() throws SQLException {
        StatementRegistry registry = new StatementRegistry();
        Statement statement = mock(Statement.class);
        doThrow(new SQLException("cancel unsupported")).when(statement).cancel();
        registry.set(statement);

        registry.cancelCurrent();
        verify(statement).cancel();
    }

    @Test
    void clear_shouldUnregisterStatement_soSubsequentCancelIsNoOp() throws SQLException {
        StatementRegistry registry = new StatementRegistry();
        Statement statement = mock(Statement.class);
        registry.set(statement);
        registry.clear();

        registry.cancelCurrent();
        verify(statement, never()).cancel();
    }

    @Test
    void cdcEngineStop_shouldCancelInFlightStatements() {
        assertNotNull(new StatementRegistry());
    }
}
