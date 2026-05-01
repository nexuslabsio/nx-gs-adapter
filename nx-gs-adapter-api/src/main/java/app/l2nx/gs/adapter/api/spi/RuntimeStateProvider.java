package app.l2nx.gs.adapter.api.spi;

import java.util.List;

/**
 * Tier-2 SPI: described once per game-server schema variant for the
 * runtime-sync module. Discovered by {@code RuntimeSyncModule} via
 * {@code ServiceLoader.load(RuntimeStateProvider.class)} once at module
 * {@code start()}. Providers ship a descriptor at
 * {@code META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider}.
 *
 * <p>Sibling of {@link DbSchemaProvider}: both Tier-2 SPIs surface entities
 * to be synced, but the data source differs — {@code DbSchemaProvider}
 * describes JDBC tables for CDC, {@link RuntimeStateProvider} describes
 * in-memory game-server stores polled per tick. The same schema variant
 * (e.g. {@code "bohpts"}) typically ships both providers in the host JAR.</p>
 *
 * <p>Resolution rule (single-impl assumption for MVP):</p>
 * <ul>
 *     <li>0 impls on classpath → runtime-sync transitions to {@code DISABLED}
 *     with an actionable WARN.</li>
 *     <li>1 impl → engine uses it.</li>
 *     <li>&gt;1 impls → runtime-sync transitions to {@code FAILED} with an
 *     actionable ERROR listing every conflicting impl class name.</li>
 * </ul>
 */
public interface RuntimeStateProvider {

    /**
     * Schema variant identifier — informational, surfaced in startup logs and
     * heartbeats. Examples: {@code "l2j"}, {@code "bohpts"},
     * {@code "lucera"}. Not a selection key in MVP (single-impl rule).
     */
    String schemaName();

    /**
     * The runtime entities this provider knows about. Order matters: the engine
     * spins up one daemon thread per entity in the returned order.
     */
    List<RuntimeEntityMapping<?>> mappings();
}
