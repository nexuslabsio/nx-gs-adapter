package app.l2nx.gs.adapter.api.spi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Tier-2 SPI: declares one child source table that contributes rows to an
 * {@link EntityMapping}. Each child references its parent entity via
 * {@link #fkColumn()} pointing at the {@link PrimarySource#pkColumn() primary's
 * PK}. An entity may have zero-or-more children
 * ({@link EntityMapping#children()}); each is one isolated SQL statement
 * — the engine never composes a JOIN across primary + children.
 *
 * <p>Engine SQL:</p>
 * <ul>
 *     <li>Phase 1: {@code SELECT fkColumn,
 *     BIT_XOR(CRC32(CONCAT_WS(',', col1, col2, ...))) FROM tableName WHERE
 *     fkColumn BETWEEN ? AND ? GROUP BY fkColumn}. The XOR aggregate is
 *     order-insensitive — child row order in the source table has no
 *     bearing on the hash. Engine XOR-folds the resulting per-FK CRC into
 *     the primary's CRC for the same PK to produce the entity's aggregate
 *     CRC.</li>
 *     <li>Phase 2: {@code SELECT * FROM tableName WHERE fkColumn IN (?, ?,
 *     ...)} (chunked); rows grouped by FK and passed to
 *     {@link EntityMapping#mapEntity} keyed by {@link #tableName()}.</li>
 * </ul>
 *
 * <p>Orphan child rows (FK with no matching primary row) are dropped silently
 * by the engine: the entity does not exist without a primary row, and child
 * rows for missing parents never reach {@code mapEntity}. Cleanup of
 * orphans is the host DB's responsibility.</p>
 *
 * <p>{@code C} is impl-private — same opacity contract as
 * {@link PrimarySource#mapRow}.</p>
 */
public interface ChildSource<C> {

    /**
     * Source SQL table for the child side (e.g. {@code "clan_skills"} for
     * the {@code "clan"} entity). Used as the lookup key in the
     * {@code childRowsByTable} map passed to
     * {@link EntityMapping#mapEntity}.
     */
    String tableName();

    /**
     * Column on {@link #tableName()} that references
     * {@link PrimarySource#pkColumn() primary's PK}. The engine reads FK
     * values as {@code long} via {@code rs.getLong(fkColumn())} and binds
     * them via {@code setLong(...)}. Same {@code long}-PK invariant as
     * {@link PrimarySource}.
     */
    String fkColumn();

    /**
     * Columns whose values feed
     * {@code BIT_XOR(CRC32(CONCAT_WS(',', col1, col2, ...)))} in Phase 1.
     * Order matters for hash stability across rebuilds. Adding a column
     * invalidates the snapshot for every entity that uses this child
     * source.
     */
    List<String> hashedColumns();

    /**
     * Phase 2 single-row mapper. Called once per child row in the
     * {@code IN(...)} chunk; engine groups returned values by FK before
     * passing them to {@link EntityMapping#mapEntity}.
     *
     * @throws SQLException re-thrown to the engine, which transitions the
     *                      affected entity to {@code DEGRADED} for the cycle.
     */
    C mapRow(ResultSet rs) throws SQLException;
}
