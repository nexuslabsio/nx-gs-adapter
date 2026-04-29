package app.l2nx.gs.adapter.api.spi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Tier-2 SPI: describes ONE synced entity (clan, character, item, …). The
 * adapter's domain vocabulary is entity-centric — operators and platform
 * consumers think in entities, not in DB tables; the source SQL table is an
 * internal-to-the-mapping detail. MVP enforces 1 entity = 1 source table;
 * future multi-table entities are an extension point.
 *
 * <p>This interface describes ONLY the schema shape ("what to sync"). All
 * operational parameters (cadence, window size, timeouts) come from
 * {@code l2nx.properties} per the cdc-engine config policy — the SPI does NOT
 * carry {@code tickInterval()} / {@code strategy()} / {@code windowCount()} /
 * {@code queryTimeout()} fields. Mixing operator concerns into the schema
 * provider would force a schema-provider rebuild every time an operator wants
 * to retune.</p>
 *
 * <p>PK is {@code long} end-to-end — engine internals (fastutil
 * {@code Long2IntOpenHashMap}), Kafka message key
 * ({@code LongSerializer}, 8 bytes binary), and {@code SyncEvent.pk: long}
 * payload all carry the raw long. Providers MUST read the PK column via
 * {@code rs.getLong(pkColumn())}. Composite-PK and non-numeric-PK entities are
 * out of scope for MVP.</p>
 */
public interface EntityMapping<T> {

    /**
     * Domain identifier in singular form: {@code "clan"}, {@code "character"},
     * {@code "item"}. Used as the lookup key into
     * {@code ConnectResponse.syncTopics} to resolve the Kafka topic for this
     * entity. Surfaced through heartbeat as {@code EntityStats.name}.
     */
    String entityName();

    /**
     * Source SQL table for the entity (e.g. {@code "clan_data"} for the
     * {@code "clan"} entity). Internal to the mapping — used only by the
     * engine's SQL {@code FROM} clause and {@code SELECT} statements; never
     * appears on the wire.
     */
    String tableName();

    /**
     * Primary-key column on {@link #tableName()}. Single-column numeric PK
     * assumption: the engine reads PK values as {@code long} via
     * {@code rs.getLong(pkColumn())} and binds them via {@code setLong(...)}.
     */
    String pkColumn();

    /**
     * Columns whose values feed CRC32 in Phase 1. The engine emits
     * {@code SELECT pk, CRC32(CONCAT_WS(',', col1, col2, ...))} with these
     * columns as the {@code CONCAT_WS} arguments — order matters for hash
     * stability across rebuilds. Adding a column to the list invalidates every
     * cached snapshot (and every persisted snapshot once that lands).
     */
    List<String> hashedColumns();

    /**
     * Phase 2 row mapper. Called once per row whose CRC32 changed; converts
     * the JDBC {@link ResultSet} cursor into a typed DTO. Implementations
     * SHOULD apply L2J sentinel-zero conventions here (e.g.
     * {@code 0 → null} for {@code leader_id} / {@code ally_id}).
     *
     * @throws SQLException re-thrown to the engine, which transitions the
     *                      affected entity to {@code DEGRADED} for the cycle.
     */
    T mapRow(ResultSet rs) throws SQLException;

    /**
     * DTO class for serialization. The engine passes this to Gson when
     * serializing the {@code SyncEvent<T>} payload. Platform consumers compile
     * against the same class for their {@code Consumer<SyncEvent<T>>}.
     *
     * <p><b>Constraint:</b> the DTO MUST be a concrete, non-parameterized class.
     * {@link Class} loses generic parameters through erasure — a hypothetical
     * {@code EntityMapping<List<X>>} would silently serialize as raw
     * {@code List} of {@code Object}. If a future entity needs a parameterized
     * payload, replace this method with a {@code TypeToken<T>} return (which
     * pulls Gson into the contract — defer until a real case demands it).</p>
     */
    Class<T> dtoType();
}
