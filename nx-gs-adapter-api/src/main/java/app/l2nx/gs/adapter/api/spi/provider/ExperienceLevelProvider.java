package app.l2nx.gs.adapter.api.spi.provider;

import app.l2nx.gs.adapter.api.kafka.sync.gd.experiencelevel.ExperienceLevel;
import java.util.Collection;

/** Source of the static experience table for {@code gd-sync}, discovered via {@link java.util.ServiceLoader}. */
public interface ExperienceLevelProvider {

    /** gd-sync entity name; always {@code "experiencelevel"}. */
    String entityName();

    /**
     * Full current set, one row per level. {@code null} means the host is not ready: the burst is aborted.
     */
    Collection<ExperienceLevel> snapshot();
}
