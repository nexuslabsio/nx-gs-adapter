package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Build-agnostic gear-score ruleset wire DTO — the global "what earns gear score and
 * how much" reference, carried as the payload of {@code GameDataSyncEvent} on the
 * {@code gd} sync stream's {@code gearscore} entity topic. A singleton per
 * {@code (tenant, server)}; the platform stores it as a document and the wiki renders
 * its {@link #getGroups() groups} as tables.
 *
 * <p>{@link #isEnabled() enabled} reflects whether the host build actually computes
 * gear score — a build that ships the ruleset entity but has gear score turned off
 * publishes {@code enabled=false} with whatever groups it knows.</p>
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

    /**
     * Whether the host build computes gear score.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Rule groups by category; never {@code null} (empty when none supplied).
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
