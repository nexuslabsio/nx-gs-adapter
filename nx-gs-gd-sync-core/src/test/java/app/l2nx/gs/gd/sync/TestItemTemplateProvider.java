package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate;
import app.l2nx.gs.adapter.api.spi.ItemTemplateProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ServiceLoader-discovered {@link ItemTemplateProvider} test double, mirroring
 * {@link TestGearScoreRulesetProvider}. Used by {@link GameDataReadinessTest} to prove the
 * readiness gate never touches a Tier-2 provider while the host is unready — {@link #callCount}
 * tracks {@link #snapshot()} invocations for that assertion.
 */
public final class TestItemTemplateProvider implements ItemTemplateProvider {

    static volatile Collection<ItemTemplate> snapshot = Collections.emptyList();
    static final AtomicInteger callCount = new AtomicInteger();

    @Override
    public String entityName() {
        return "itemtemplate";
    }

    @Override
    public Collection<ItemTemplate> snapshot() {
        callCount.incrementAndGet();
        return snapshot;
    }
}
