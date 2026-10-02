package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.GearScoreRuleset;
import app.l2nx.gs.adapter.api.spi.provider.GearScoreRulesetProvider;
import java.util.Optional;

/** Static mutable snapshot lets tests flip between one ruleset and {@link Optional#empty()}. */
public final class TestGearScoreRulesetProvider implements GearScoreRulesetProvider {

    static volatile Optional<GearScoreRuleset> snapshot = Optional.empty();

    @Override
    public String entityName() {
        return "gearscore";
    }

    @Override
    public Optional<GearScoreRuleset> snapshot() {
        return snapshot;
    }
}
