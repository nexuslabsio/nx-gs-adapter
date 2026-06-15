package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.instancetemplate.InstanceTemplate;

import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of static instance-zone (reflection)
 * name data for the {@code gd-sync} module. Defined here in
 * {@code nx-gs-adapter-api} so client providers depend only on the contracts
 * artifact.
 *
 * <p>The host implements this over its already-parsed reflection-name catalog
 * (e.g. an L2J/Lucera {@code reflectionNames.xml} holder) and maps each entry
 * into the build-agnostic {@link InstanceTemplate}. Discovered via
 * {@link java.util.ServiceLoader} exactly like {@code ItemTemplateProvider};
 * the {@code gd-sync} module pulls a fresh {@link #snapshot()} on connect and
 * whenever the host triggers a re-publish.</p>
 *
 * <p>Implementations MUST be safe to call after host boot completes (catalog
 * fully loaded) and SHOULD be cheap to call repeatedly — the module owns
 * snapshot/diff/publish; the provider only yields the current template set.</p>
 */
public interface InstanceTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always
     * {@code "instance"}. Used to resolve the Kafka topic from
     * {@code ctx.getSyncTopics().getGd()} and to tag the wire envelope.
     */
    String entityName();

    /**
     * The full current set of instance templates. Returned in one shot; the
     * module keys, diffs and publishes them. Never {@code null} (empty is valid).
     */
    Collection<InstanceTemplate> snapshot();
}
