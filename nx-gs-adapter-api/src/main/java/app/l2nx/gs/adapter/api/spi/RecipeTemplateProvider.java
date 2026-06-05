package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.recipetemplate.RecipeTemplate;

import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of static crafting-recipe data for the
 * {@code gd-sync} module. Defined here in {@code nx-gs-adapter-api} so client providers
 * depend only on the contracts artifact.
 *
 * <p>The host implements this over its already-parsed in-memory recipe catalog (e.g. an
 * L2J/Lucera {@code RecipeParser}), mapping each recipe into the build-agnostic
 * {@link RecipeTemplate} aggregate (header + ingredient list). Discovered via
 * {@link java.util.ServiceLoader} exactly like {@code ItemTemplateProvider}; the
 * {@code gd-sync} module pulls a fresh {@link #snapshot()} on connect and whenever the
 * host triggers a re-publish.</p>
 */
public interface RecipeTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always {@code "recipetemplate"}.
     * Used to resolve the Kafka topic from {@code ctx.getSyncTopics().getGd()} and to tag
     * the wire envelope.
     */
    String entityName();

    /**
     * The full current set of recipes (one aggregate per recipe id). Returned in one shot;
     * the module keys, diffs and publishes them. Never {@code null} (empty is valid).
     */
    Collection<RecipeTemplate> snapshot();
}
