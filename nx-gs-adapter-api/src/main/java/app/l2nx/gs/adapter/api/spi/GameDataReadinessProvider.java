package app.l2nx.gs.adapter.api.spi;

/**
 * Optional Tier-2 SPI telling the {@code gd-sync} module whether the host's static game-data
 * catalogs (item / npc / skill / … templates) are loaded and safe to read. Registered via
 * {@code META-INF/services}; at most one implementation may be present.
 *
 * <p><b>No implementation registered means "always ready"</b> — every host that predates this SPI
 * keeps its previous behaviour.</p>
 *
 * <p>The adapter connects during host boot, before the datapack has been parsed. Without this
 * signal the module would pull a snapshot from providers that have nothing yet, which is both
 * useless and dangerous: reading them force-loads the host's parsers off its boot thread and out of
 * order, and a provider that answers with an empty collection instead of {@code null} makes the
 * module publish a {@code SNAPSHOT_COMPLETE count=0} marker, whose reconcile pass deletes that
 * entity's whole catalog on the platform. So the module asks here first and skips the pass
 * entirely while the answer is {@code false}.</p>
 *
 * <p>Readiness is host-wide rather than per-entity because the catalogs come from one datapack
 * load. The module polls this method from its own scheduler thread, so implementations MUST be
 * cheap, non-blocking and callable from any thread — typically a read of a volatile flag flipped at
 * the end of host boot.</p>
 */
public interface GameDataReadinessProvider {

    /**
     * {@code true} once the host's game-data catalogs are fully loaded and its
     * {@code gd-sync} Tier-2 providers can return real snapshots.
     */
    boolean ready();
}
