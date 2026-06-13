package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.classtemplate.ClassTemplate;

import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of the static playable-class catalog for the
 * {@code gd-sync} module. Defined here in {@code nx-gs-adapter-api} so client providers depend
 * only on the contracts artifact.
 *
 * <p>The host implements this over its already-parsed in-memory class registry (e.g. an
 * L2J/Lucera {@code ClassId} enum), emitting one build-agnostic {@link ClassTemplate} per
 * playable class (race / type / tier facets + the profession-parent link). Discovered via
 * {@link java.util.ServiceLoader} exactly like {@code SkillTemplateProvider}; the
 * {@code gd-sync} module pulls a fresh {@link #snapshot()} on connect and whenever the host
 * triggers a re-publish.</p>
 *
 * <p>Implementations MUST be safe to call after host boot completes (class catalog fully loaded)
 * and SHOULD be cheap to call repeatedly — the module owns snapshot/diff/publish; the provider
 * only yields the current class set.</p>
 */
public interface ClassTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always {@code "classtemplate"}.
     * Used to resolve the Kafka topic from {@code ctx.getSyncTopics().getGd()} and to tag the
     * wire envelope.
     */
    String entityName();

    /**
     * The full current set of playable classes (one aggregate per class id). Returned in one
     * shot; the module keys, diffs and publishes them. Never {@code null} (empty is valid).
     */
    Collection<ClassTemplate> snapshot();
}
