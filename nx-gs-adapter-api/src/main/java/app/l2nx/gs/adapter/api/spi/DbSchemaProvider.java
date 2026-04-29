package app.l2nx.gs.adapter.api.spi;

import java.util.List;

/**
 * Tier-2 SPI: described once per game-server schema variant (vanilla L2J,
 * bohpts, Lucera, …). Discovered by {@code DbSyncModule} via
 * {@code ServiceLoader.load(DbSchemaProvider.class)} once at module
 * {@code start()}. Providers ship a descriptor at
 * {@code META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider}.
 *
 * <p>Resolution rule (single-impl assumption for MVP):</p>
 * <ul>
 *     <li>0 impls on classpath → db-sync transitions to {@code DISABLED} with
 *     an actionable WARN.</li>
 *     <li>1 impl → engine uses it.</li>
 *     <li>&gt;1 impls → db-sync transitions to {@code FAILED} with an
 *     actionable ERROR listing every conflicting impl class name.</li>
 * </ul>
 *
 * <p>The provider describes ONLY the schema shape ("what to sync"). Engine
 * runtime parameters (tick interval, window size, query timeout) are sourced
 * exclusively from {@code l2nx.properties} (operator-owned); providers do NOT
 * declare them.</p>
 */
public interface DbSchemaProvider {

    /**
     * Schema variant identifier — informational, surfaced in startup logs and
     * heartbeats. Examples: {@code "l2j"}, {@code "bohpts"},
     * {@code "lucera"}. Not a selection key in MVP (single-impl rule).
     */
    String schemaName();

    /**
     * The entities this provider knows about. Order matters: the engine spins
     * up one scheduler thread per entity in the returned order. Providers with
     * cross-entity ordering preferences (e.g. small-and-fast first) arrange the
     * list manually — the engine does NOT sort by row count.
     */
    List<EntityMapping<?>> mappings();
}
