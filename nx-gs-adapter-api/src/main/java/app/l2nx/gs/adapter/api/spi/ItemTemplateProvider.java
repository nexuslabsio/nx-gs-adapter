package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate;
import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of static item-template data for the
 * {@code gd-sync} module. Defined here in {@code nx-gs-adapter-api} so client
 * providers depend only on the contracts artifact.
 *
 * <p>The host implements this over its already-parsed in-memory item catalog
 * (e.g. an L2J/Lucera {@code ItemsParser}) and maps each core item into the
 * build-agnostic {@link ItemTemplate}. Discovered via {@link java.util.ServiceLoader}
 * exactly like {@code DbSchemaProvider}; the {@code gd-sync} module pulls a fresh
 * {@link #snapshot()} on connect and whenever the host triggers a re-publish.</p>
 *
 * <p>Implementations MUST be safe to call after host boot completes (item catalog
 * fully loaded) and SHOULD be cheap to call repeatedly — the module owns
 * snapshot/diff/publish; the provider only yields the current template set.</p>
 */
public interface ItemTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always
     * {@code "itemtemplate"}. Used to resolve the Kafka topic from
     * {@code ctx.getSyncTopics().getGd()} and to tag the wire envelope. Mirrors
     * {@code EntityMapping.entityName()} on the db-sync side.
     */
    String entityName();

    /**
     * The full current set of item templates. Returned in one shot; the module
     * keys, diffs and publishes them. Never {@code null} (empty is valid).
     */
    Collection<ItemTemplate> snapshot();
}
