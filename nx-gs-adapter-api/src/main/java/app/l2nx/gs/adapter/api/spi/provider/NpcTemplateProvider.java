package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate.NpcTemplate;
import java.util.Collection;

/**
 * Source of static NPC-template data for the {@code gd-sync} module, discovered via {@link java.util.ServiceLoader}.
 * The module pulls a fresh {@link #snapshot()} on connect and on every host-triggered re-publish.
 *
 * <p>Must be safe to call after host boot completes and cheap to call repeatedly.</p>
 */
public interface NpcTemplateProvider {

    /**
     * gd-sync entity name (always {@code "npctemplate"}); resolves the Kafka topic via
     * {@code ctx.getSyncTopics().getGd()}.
     */
    String entityName();

    /**
     * Full current set, returned in one shot. Never {@code null}; empty is valid.
     */
    Collection<NpcTemplate> snapshot();
}
