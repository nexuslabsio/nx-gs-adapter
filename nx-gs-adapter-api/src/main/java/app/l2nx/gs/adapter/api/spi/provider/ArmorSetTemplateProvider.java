package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate.ArmorSetTemplate;
import java.util.Collection;

/**
 * Source of static armor-set data for the {@code gd-sync} module, discovered via {@link java.util.ServiceLoader}.
 * The module pulls a fresh {@link #snapshot()} on connect and on every host-triggered re-publish.
 */
public interface ArmorSetTemplateProvider {

    /**
     * gd-sync entity name (always {@code "armorsettemplate"}); resolves the Kafka topic via
     * {@code ctx.getSyncTopics().getGd()}.
     */
    String entityName();

    /**
     * Full current set, returned in one shot. Never {@code null}; empty is valid.
     */
    Collection<ArmorSetTemplate> snapshot();
}
