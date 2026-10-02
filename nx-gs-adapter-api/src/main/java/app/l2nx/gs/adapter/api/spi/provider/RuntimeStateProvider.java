package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.spi.model.RuntimeEntityMapping;
import java.util.List;

/**
 * Describes the runtime-synced entities for one game-server schema variant; loaded by
 * {@code RuntimeSyncModule} via {@link java.util.ServiceLoader} at {@code start()}. Counterpart of
 * {@link DbSchemaProvider} for in-memory state.
 *
 * <p>Exactly one impl is supported: none disables runtime-sync ({@code DISABLED}), several fail it
 * ({@code FAILED}).</p>
 */
public interface RuntimeStateProvider {

    /**
     * Informational variant name (e.g. {@code "bohpts"}) for logs and heartbeats.
     */
    String schemaName();

    /**
     * Entities to sync; the engine starts one daemon thread per entity in list order.
     */
    List<RuntimeEntityMapping<?>> mappings();
}
