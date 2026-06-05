package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate.ArmorSetTemplate;

import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of static armor-set data for the {@code gd-sync}
 * module. Defined here in {@code nx-gs-adapter-api} so client providers depend only on the
 * contracts artifact.
 *
 * <p>The host implements this over its already-parsed in-memory armor-set catalog (e.g. an
 * L2J/Lucera {@code ArmorSetsParser}), mapping each set into the build-agnostic
 * {@link ArmorSetTemplate} aggregate (per-slot items + granted skills + stat bonuses).
 * Discovered via {@link java.util.ServiceLoader} exactly like {@code ItemTemplateProvider};
 * the {@code gd-sync} module pulls a fresh {@link #snapshot()} on connect and whenever the
 * host triggers a re-publish.</p>
 */
public interface ArmorSetTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always {@code "armorsettemplate"}.
     * Used to resolve the Kafka topic from {@code ctx.getSyncTopics().getGd()} and to tag
     * the wire envelope.
     */
    String entityName();

    /**
     * The full current set of armor sets (one aggregate per set id). Returned in one shot;
     * the module keys, diffs and publishes them. Never {@code null} (empty is valid).
     */
    Collection<ArmorSetTemplate> snapshot();
}
