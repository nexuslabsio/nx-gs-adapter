package app.l2nx.gs.adapter.api.spi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Tier-2 SPI: declares the primary source table for an
 * {@link EntityMapping}. Drives windowing (engine reads
 * {@code MIN/MAX(pkColumn())} from {@link #tableName()} at the start of every
 * cycle) and entity identity (every entity DTO is keyed by the primary's
 * {@code long} PK).
 *
 * <p>Engine SQL (see {@code cdc-engine/spec.md} R1):</p>
 * <ul>
 *     <li>Phase 1: {@code SELECT pkColumn, CRC32(CONCAT_WS(',', col1, col2,
 *     ...)) FROM tableName WHERE pkColumn BETWEEN ? AND ?}.</li>
 *     <li>Phase 2: {@code SELECT * FROM tableName WHERE pkColumn IN (?, ?,
 *     ...)} (chunked).</li>
 * </ul>
 *
 * <p>{@code P} is impl-private — the engine treats per-source rows as opaque
 * {@link Object} between {@link #mapRow} and the entity-level
 * {@link EntityMapping#mapEntity}. Casting back to {@code P} is the impl's
 * responsibility (it owns both sides).</p>
 */
public interface PrimarySource<P> {

    /**
     * Source SQL table for the entity's primary side (e.g. {@code "clan_data"}
     * for the {@code "clan"} entity). Internal to the mapping — used only by
     * the engine's {@code FROM} / {@code SELECT} clauses; never appears on the
     * wire.
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
     * Phase 2 single-row mapper. Called once per row whose CRC32 changed (the
     * engine fetches primary rows for {@code created ∪ updated} PKs in a
     * cycle). Implementations SHOULD apply L2J sentinel-zero conventions here
     * (e.g. {@code 0 → null} for {@code leader_id} / {@code ally_id}) and
     * return a private row record that {@link EntityMapping#mapEntity} casts
     * back.
     *
     * @throws SQLException re-thrown to the engine, which transitions the
     *                      affected entity to {@code DEGRADED} for the cycle.
     */
    P mapRow(ResultSet rs) throws SQLException;
}
