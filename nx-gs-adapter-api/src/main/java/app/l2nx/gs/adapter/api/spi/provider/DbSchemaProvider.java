package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.spi.model.EntityMapping;
import java.util.List;

/**
 * Describes the entities to sync for one game-server schema variant; loaded by {@code DbSyncModule}
 * via {@link java.util.ServiceLoader} at {@code start()}. Shape only: engine parameters come from
 * {@code l2nx.properties}.
 *
 * <p>Exactly one impl is supported: none disables db-sync ({@code DISABLED}), several fail it
 * ({@code FAILED}).</p>
 */
public interface DbSchemaProvider {

    /**
     * Informational variant name (e.g. {@code "my-host"}) for logs and heartbeats.
     */
    String schemaName();

    /**
     * Entities to sync; the engine starts one scheduler thread per entity in list order and does not
     * sort by row count.
     */
    List<EntityMapping<?>> mappings();
}
