package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.GearScoreRuleset;
import app.l2nx.gs.adapter.api.spi.GearScoreRulesetProvider;
import java.util.Optional;

/**
 * ServiceLoader-discovered {@link GearScoreRulesetProvider} test double. The module resolves
 * its providers through a real {@link java.util.ServiceLoader}, so the only way to exercise the
 * registered {@code gearscore} descriptor end-to-end is to put a real impl on the test classpath
 * (registered via {@code META-INF/services}). The returned snapshot is a static mutable holder so
 * each test can flip between the one-ruleset and the {@link Optional#empty()} (gear score disabled)
 * cases without a separate registration.
 */
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
