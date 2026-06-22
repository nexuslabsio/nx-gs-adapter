package app.l2nx.gs.db.sync.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JdbcDialectTest {

    @ParameterizedTest
    @CsvSource({
        "jdbc:mysql://localhost/db,                                       MYSQL",
        "jdbc:mysql:loadbalance://h1:3306/db,                             MYSQL",
        "jdbc:mariadb://localhost/db,                                     MARIADB",
        "JDBC:MariaDB://localhost/db,                                     MARIADB",
        "jdbc:postgresql://localhost/db,                                  POSTGRES",
        "jdbc:postgres://localhost/db,                                    POSTGRES",
        "jdbc:h2:mem:test,                                                OTHER",
        "jdbc:sqlite::memory:,                                            OTHER",
    })
    void detect_shouldClassifyByUrlPrefix(String url, JdbcDialect expected) throws SQLException {
        Connection conn = mock(Connection.class);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(md);
        when(md.getURL()).thenReturn(url);

        assertEquals(expected, JdbcDialect.detect(conn));
    }

    @Test
    void detect_shouldReturnOther_whenUrlNull() throws SQLException {
        Connection conn = mock(Connection.class);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(conn.getMetaData()).thenReturn(md);
        when(md.getURL()).thenReturn(null);

        assertEquals(JdbcDialect.OTHER, JdbcDialect.detect(conn));
    }

    @Test
    void detect_shouldReturnOther_whenGetMetaDataThrows() throws SQLException {
        Connection conn = mock(Connection.class);
        when(conn.getMetaData()).thenThrow(new SQLException("boom"));

        assertEquals(JdbcDialect.OTHER, JdbcDialect.detect(conn));
    }
}
