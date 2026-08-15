package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.spi.GameDataReadinessProvider;

/**
 * ServiceLoader-discovered {@link GameDataReadinessProvider} test double, mirroring
 * {@link TestGearScoreRulesetProvider}. Defaults to {@code ready=true} so every other test class
 * sharing this module's test classpath (e.g. {@link GearScoreDescriptorTest}) sees
 * production-equivalent "always ready" behaviour unless a test in {@link GameDataReadinessTest}
 * explicitly flips the flag — which it MUST restore in {@code @AfterEach}.
 */
public final class TestGameDataReadinessProvider implements GameDataReadinessProvider {

    static volatile boolean ready = true;

    @Override
    public boolean ready() {
        return ready;
    }
}
