package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.GearScoreRuleset;

import java.util.Optional;

/**
 * Tier-2 SPI: the host build's source of the global gear-score ruleset for the
 * {@code gd-sync} module. Defined here in {@code nx-gs-adapter-api} so client
 * providers depend only on the contracts artifact.
 *
 * <p>The host implements this over its gear-score configuration (rates, enchant
 * profiles, scaling tables) and maps it into the single build-agnostic
 * {@link GearScoreRuleset}. Discovered via {@link java.util.ServiceLoader} exactly
 * like {@code ItemTemplateProvider} / {@code SkillProvider}; the {@code gd-sync}
 * module pulls a fresh {@link #snapshot()} on connect and whenever the host triggers
 * a re-publish.</p>
 *
 * <p>Unlike the catalog providers this entity is a singleton — {@link #snapshot()}
 * returns at most one ruleset. A build with no gear-score system simply does not
 * register the provider, or returns {@link Optional#empty()}.</p>
 */
public interface GearScoreRulesetProvider {

    /**
     * The gd-sync entity name this provider feeds — {@code "gearscore"}.
     * Used to resolve the Kafka topic from {@code ctx.getSyncTopics().getGd()} and to
     * tag the wire envelope.
     */
    String entityName();

    /**
     * The current gear-score ruleset, or {@link Optional#empty()} when the build has
     * no gear-score system. The module keys, diffs and publishes the single value.
     */
    Optional<GearScoreRuleset> snapshot();
}
