package app.l2nx.gs.adapter.api.spi.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * One synced entity: a {@link PrimarySource} plus zero or more {@link ChildSource}s, one SQL
 * statement each. Per-PK CRC32s are XOR-folded in-engine. Schema shape only; cadence, window size
 * and timeouts come from {@code l2nx.properties}. PK is {@code long}; composite or non-numeric PKs
 * are unsupported.
 */
public interface EntityMapping<T> {

    /**
     * Singular domain name, key into {@code ConnectResponse.syncTopics}; surfaced as
     * {@code EntityStats.name}.
     */
    String entityName();

    /**
     * Gson serialization type for {@code SyncEvent<T>}; must be a concrete, non-parameterized class
     * (erasure drops generics).
     */
    Class<T> dtoType();

    /**
     * Drives windowing and entity identity.
     */
    PrimarySource<?> primary();

    /**
     * Child sources joined by FK to {@link #primary()}; empty for single-table entities.
     */
    List<ChildSource<?>> children();

    /**
     * Other entities this one belongs to via an FK on its primary table. Unlike {@link #children()},
     * a parent ref points to a separate sync stream. The force-resync cascade follows these refs;
     * default empty means never cascaded into.
     */
    default List<ParentRef> parentRefs() {
        return Collections.emptyList();
    }

    /**
     * Assembles the DTO for a created or updated PK (never deletions). {@code primaryRow} is
     * non-null: if the row vanished between phases the engine drops the publish and the next cycle
     * emits a delete. {@code childRowsByTable} has a key for every declared child, possibly with
     * an empty list. Rows are the opaque values from the impl's own {@code mapRow}s, so casting is safe.
     */
    T mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable);
}
