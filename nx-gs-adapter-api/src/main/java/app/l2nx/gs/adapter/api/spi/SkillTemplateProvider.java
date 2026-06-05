package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate.SkillTemplate;

import java.util.Collection;

/**
 * Tier-2 SPI: the host build's source of static skill data for the {@code gd-sync}
 * module. Defined here in {@code nx-gs-adapter-api} so client providers depend only on
 * the contracts artifact.
 *
 * <p>The host implements this over its already-parsed in-memory skill catalog (e.g. an
 * L2J/Lucera {@code SkillsParser}), grouping the core's per-level skill objects by
 * {@code skillId} into the build-agnostic {@link SkillTemplate} aggregate (base level ladder +
 * enchant-route variants + per-level effects). Discovered via
 * {@link java.util.ServiceLoader} exactly like {@code ItemTemplateProvider}; the
 * {@code gd-sync} module pulls a fresh {@link #snapshot()} on connect and whenever the
 * host triggers a re-publish.</p>
 *
 * <p>Implementations MUST be safe to call after host boot completes (skill catalog fully
 * loaded) and SHOULD be cheap to call repeatedly — the module owns
 * snapshot/diff/publish; the provider only yields the current skill set.</p>
 */
public interface SkillTemplateProvider {

    /**
     * The gd-sync entity name this provider feeds — currently always {@code "skilltemplate"}.
     * Used to resolve the Kafka topic from {@code ctx.getSyncTopics().getGd()} and to
     * tag the wire envelope.
     */
    String entityName();

    /**
     * The full current set of skills (one aggregate per {@code skillId}). Returned in
     * one shot; the module keys, diffs and publishes them. Never {@code null} (empty is
     * valid).
     */
    Collection<SkillTemplate> snapshot();
}
