package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.kafka.sync.gd.classtemplate.ClassTemplate;
import java.util.Collection;

/**
 * Source of static playable-class data for the {@code gd-sync} module, discovered via {@link java.util.ServiceLoader}.
 * The module pulls a fresh {@link #snapshot()} on connect and on every host-triggered re-publish.
 *
 * <p>Must be safe to call after host boot completes and cheap to call repeatedly.</p>
 */
public interface ClassTemplateProvider {

    /**
     * gd-sync entity name (always {@code "classtemplate"}); resolves the Kafka topic via
     * {@code ctx.getSyncTopics().getGd()}.
     */
    String entityName();

    /**
     * Full current set, returned in one shot. Never {@code null}; empty is valid.
     */
    Collection<ClassTemplate> snapshot();
}
