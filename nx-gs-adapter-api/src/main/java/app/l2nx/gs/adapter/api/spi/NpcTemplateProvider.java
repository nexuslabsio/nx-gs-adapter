package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate.NpcTemplate;

import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of static NPC-template data for the
 * {@code gd-sync} module. Defined here in {@code nx-gs-adapter-api} so client
 * providers depend only on the contracts artifact.
 *
 * <p>The host implements this over its already-parsed in-memory NPC catalog (e.g.
 * an L2J/Lucera {@code NpcsParser}) and maps each core NPC into the build-agnostic
 * {@link NpcTemplate}, attaching its spawn definitions from the core's spawn
 * registry. Discovered via {@link java.util.ServiceLoader} exactly like
 * {@code ItemTemplateProvider}; the {@code gd-sync} module pulls a fresh
 * {@link #snapshot()} on connect and whenever the host triggers a re-publish.</p>
 *
 * <p>Implementations MUST be safe to call after host boot completes (NPC + spawn
 * catalogs fully loaded) and SHOULD be cheap to call repeatedly — the module owns
 * snapshot/diff/publish; the provider only yields the current template set.</p>
 */
public interface NpcTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always {@code "npctemplate"}.
     * Used to resolve the Kafka topic from {@code ctx.getSyncTopics().getGd()} and to
     * tag the wire envelope.
     */
    String entityName();

    /**
     * The full current set of NPC templates. Returned in one shot; the module keys,
     * diffs and publishes them. Never {@code null} (empty is valid).
     */
    Collection<NpcTemplate> snapshot();
}
