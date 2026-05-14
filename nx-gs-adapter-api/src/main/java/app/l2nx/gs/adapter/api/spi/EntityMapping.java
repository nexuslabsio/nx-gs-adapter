package app.l2nx.gs.adapter.api.spi;

import java.util.List;
import java.util.Map;

/**
 * Tier-2 SPI: describes ONE synced entity (clan, character, item, …). The
 * adapter's domain vocabulary is entity-centric — operators and platform
 * consumers think in entities, not in DB tables; the source tables are
 * internal-to-the-mapping details.
 *
 * <p>An entity is composed of one {@link PrimarySource} (drives windowing +
 * identity) and zero-or-more {@link ChildSource}s (each carries an FK back
 * to the primary's PK). The CDC engine emits one SQL statement per source
 * — never a JOIN — and combines results in-engine: per-cycle, per-window,
 * the primary's CRC32 is XOR-folded with each child's
 * {@code BIT_XOR(CRC32(...))} aggregate to produce the entity's per-PK
 * aggregate CRC. Orphan child rows (FK with no matching primary row) are
 * dropped silently.</p>
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
 * {@code Long2IntAVLTreeMap}), Kafka message key
 * ({@code LongSerializer}, 8 bytes binary), and {@code SyncEvent.pk: long}
 * payload all carry the raw long. Composite-PK and non-numeric-PK entities
 * are out of scope.</p>
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

    /**
     * The entity's primary source table. Drives windowing (engine reads
     * {@code MIN/MAX} of the primary's PK at the start of every cycle) and
     * defines entity identity (every entity DTO is keyed by the primary's
     * PK).
     */
    PrimarySource<?> primary();

    /**
     * Additional source tables that contribute rows to entity assembly via
     * FK back to {@link #primary()}'s PK column. May be empty
     * ({@code Collections.emptyList()}) for single-table entities — the
     * engine simply skips the per-child loops in that case. Order matters
     * only for {@link #mapEntity} (the impl decides how to fold the
     * keyed-by-tableName map back into the DTO).
     */
    List<ChildSource<?>> children();

    /**
     * Phase-2 entity assembly hook. Called once per created or updated PK
     * (never for deletions — tombstones have {@code payload=null}). The
     * engine has already fetched and grouped per-source rows:
     *
     * <ul>
     *     <li>{@code primaryRow} — the value produced by
     *     {@code primary().mapRow(rs)} for this PK. Never {@code null} for a
     *     created/updated PK; if the primary row vanished between Phase 1
     *     and Phase 2 the engine drops the publish silently and the next
     *     cycle handles it as a delete.</li>
     *     <li>{@code childRowsByTable} — keyed by {@link ChildSource#tableName()};
     *     the value is the (possibly empty) list of rows produced by that
     *     child's {@code mapRow} for the given parent PK. The engine
     *     guarantees a present key for every declared child, even if the
     *     list is empty.</li>
     * </ul>
     *
     * <p>Implementations cast the opaque {@link Object} rows back to their
     * private record types (the impl owns both the {@code mapRow} producers
     * and the {@code mapEntity} consumer, so the cast is impl-local
     * type-safe) and return the typed entity DTO {@code T}.</p>
     */
    T mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable);
}
