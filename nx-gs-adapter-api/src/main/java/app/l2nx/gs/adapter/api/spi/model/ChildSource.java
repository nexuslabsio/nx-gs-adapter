package app.l2nx.gs.adapter.api.spi.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * One child source table of an {@link EntityMapping}, joined to the primary by FK; the engine
 * never JOINs. Orphan child rows (no primary row) are dropped before {@code mapEntity}.
 *
 * <p>{@code C} is impl-private, opaque to the engine like {@link PrimarySource}'s row type.</p>
 */
public interface ChildSource<C> {

    /**
     * Key of this child's rows in the {@code childRowsByTable} map of {@link EntityMapping#mapEntity}.
     */
    String tableName();

    /**
     * FK to {@link PrimarySource#pkColumn()}; read and bound as {@code long}.
     */
    String fkColumn();

    /**
     * Columns fed to {@code BIT_XOR(CRC32(CONCAT_WS(',', ...)))}. Order matters for hash stability;
     * adding a column invalidates the snapshot of every entity using this child.
     */
    List<String> hashedColumns();

    /**
     * Maps one child row; the engine groups results by FK.
     *
     * @throws SQLException marks the affected entity {@code DEGRADED} for the cycle
     */
    C mapRow(ResultSet rs) throws SQLException;
}
