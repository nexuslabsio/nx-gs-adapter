package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Tier-3 SPI: source of read-only JDBC {@link Connection}s for DB-reading adapter
 * modules (db-sync first, future metrics modules). Implementations ship a descriptor
 * at {@code META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource}.
 *
 * <p>The intended pattern is a host-JVM impl that wraps the host's already-running
 * connection pool — no second pool, no duplicated credentials. Example for bohpts:</p>
 * <pre>
 *   public final class BohptsJdbcConnectionSource implements JdbcConnectionSource {
 *       public String name() { return "bohpts-hikari"; }
 *       public Connection getConnection() throws SQLException {
 *           return DatabaseFactory.getInstance().getConnection();
 *       }
 *   }
 * </pre>
 *
 * <p>Consumers MUST call {@link Connection#setReadOnly(boolean) setReadOnly(true)}
 * immediately after every borrow — the SPI does not impose pool-level configuration
 * because the pool is host-owned. Connections are returned via
 * {@code try-with-resources}.</p>
 *
 * <p>Providers MAY pre-set {@code readOnly=true} on the borrowed connection inside
 * {@link #getConnection()} as defense-in-depth (close the borrowed connection on
 * failure so it doesn't leak). The host pool resets connection state on return,
 * so this does not affect non-adapter consumers of the same pool.</p>
 */
public interface JdbcConnectionSource {

    /**
     * Provider identifier for logging. Examples: {@code "bohpts-hikari"},
     * {@code "l2j-vanilla-dbcp2"}. Not a selection key in MVP — informational only.
     */
    String name();

    /**
     * Borrow a connection from the underlying pool. Caller is responsible for
     * closing via {@code try-with-resources}.
     */
    Connection getConnection() throws SQLException;

    /**
     * Optional pool stats for heartbeat enrichment. Default returns
     * {@link Optional#empty()} — pools that don't expose busy/idle counts simply
     * skip this method. Wrappers around HikariCP / DBCP2 / etc. typically forward
     * the host pool's MXBean accessors.
     */
    default Optional<PoolStats> stats() {
        return Optional.empty();
    }

    /**
     * Optional health probe for consumers that want to skip a tick when the pool
     * is known-down. Default {@code true} — consumers fall back to
     * {@link #getConnection()} succeeding-or-failing as their health signal.
     */
    default boolean isHealthy() {
        return true;
    }
}
