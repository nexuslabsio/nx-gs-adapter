package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.sync.gd.experiencelevel.ExperienceLevel;
import app.l2nx.gs.adapter.api.spi.provider.ExperienceLevelProvider;
import java.util.Collection;
import java.util.Collections;

public final class TestExperienceLevelProvider implements ExperienceLevelProvider {

    static volatile Collection<ExperienceLevel> snapshot = Collections.emptyList();

    @Override
    public String entityName() {
        return "experiencelevel";
    }

    @Override
    public Collection<ExperienceLevel> snapshot() {
        return snapshot;
    }
}
