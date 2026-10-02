package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.kafka.ops.model.PoolStats;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Source of read-only JDBC {@link Connection}s for DB-reading adapter modules, discovered via
 * {@link java.util.ServiceLoader}. Intended to wrap the host's existing pool, not open a second one.
 *
 * <p>Consumers call {@link Connection#setReadOnly(boolean) setReadOnly(true)} after every borrow,
 * since the pool is host-owned. Providers may pre-set it as defense in depth, closing the connection
 * on failure.</p>
 */
public interface JdbcConnectionSource {

    /**
     * Informational name for logging (e.g. {@code "bohpts-hikari"}).
     */
    String name();

    /**
     * Borrows a pooled connection; the caller closes it.
     */
    Connection getConnection() throws SQLException;

    /**
     * Pool stats for heartbeats; empty when the pool exposes none.
     */
    default Optional<PoolStats> stats() {
        return Optional.empty();
    }

    /**
     * Probe to skip a tick while the pool is known-down; default {@code true} leaves
     * {@link #getConnection()} outcome as the health signal.
     */
    default boolean isHealthy() {
        return true;
    }
}
