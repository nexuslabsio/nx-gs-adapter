package app.l2nx.gs.adapter.api.spi;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcConnectionSourceTest {

    @Test
    void defaultStats_shouldBeEmpty() {
        assertFalse(stub().stats().isPresent());
    }

    @Test
    void defaultIsHealthy_shouldBeTrue() {
        assertTrue(stub().isHealthy());
    }

    private static JdbcConnectionSource stub() {
        return new JdbcConnectionSource() {
            @Override
            public String name() {
                return "stub";
            }

            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("not used");
            }
        };
    }
}
