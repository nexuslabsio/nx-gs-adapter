package app.l2nx.gs.adapter.api.spi.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Primary source table of an {@link EntityMapping}: the engine windows over
 * {@code MIN/MAX(pkColumn())} and keys every entity DTO by this {@code long} PK.
 *
 * <p>{@code P} is impl-private; the engine passes rows as opaque {@link Object} from
 * {@link #mapRow} to {@link EntityMapping#mapEntity}.</p>
 */
public interface PrimarySource<P> {

    /**
     * Source SQL table (e.g. {@code "clan_data"}); never appears on the wire.
     */
    String tableName();

    /**
     * Single-column numeric PK, read as {@code long} and bound via {@code setLong}.
     */
    String pkColumn();

    /**
     * Columns fed to {@code CRC32(CONCAT_WS(',', ...))}. Order matters for hash stability;
     * adding a column invalidates every snapshot.
     */
    List<String> hashedColumns();

    /**
     * Maps one row whose CRC32 changed. Apply source sentinel conventions here
     * (e.g. {@code leader_id} {@code 0 -> null}).
     *
     * @throws SQLException marks the affected entity {@code DEGRADED} for the cycle
     */
    P mapRow(ResultSet rs) throws SQLException;
}
