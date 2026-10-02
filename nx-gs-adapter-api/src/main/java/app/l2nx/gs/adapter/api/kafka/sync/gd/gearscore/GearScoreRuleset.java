package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore;

import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.model.GearScoreRuleGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Singleton per {@code (tenant, server)} describing what earns gear score; the wiki renders groups as tables.
 * {@code enabled=false} when the build ships the entity but has gear score turned off.
 */
public final class GearScoreRuleset {

    private final boolean enabled;
    private final List<GearScoreRuleGroup> groups;

    public GearScoreRuleset(boolean enabled, @Nullable List<GearScoreRuleGroup> groups) {
        this.enabled = enabled;
        this.groups = groups == null
                ? Collections.<GearScoreRuleGroup>emptyList()
                : Collections.unmodifiableList(new ArrayList<GearScoreRuleGroup>(groups));
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Never {@code null}.
     */
    public List<GearScoreRuleGroup> getGroups() {
        return groups;
    }

    public Builder toBuilder() {
        return new Builder().enabled(enabled).groups(groups);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GearScoreRuleset)) return false;
        GearScoreRuleset that = (GearScoreRuleset) o;
        return enabled == that.enabled && groups.equals(that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, groups);
    }

    @Override
    public String toString() {
        return "GearScoreRuleset[enabled=" + enabled + ", groups=" + groups.size() + "]";
    }

    public static final class Builder {
        private boolean enabled;
        private @Nullable List<GearScoreRuleGroup> groups;

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder groups(@Nullable List<GearScoreRuleGroup> groups) {
            this.groups = groups;
            return this;
        }

        public GearScoreRuleset build() {
            return new GearScoreRuleset(enabled, groups);
        }
    }
}
