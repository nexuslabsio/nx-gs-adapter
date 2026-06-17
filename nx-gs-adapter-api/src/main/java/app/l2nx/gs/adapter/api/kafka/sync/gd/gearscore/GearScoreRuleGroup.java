package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A category of gear-score rules inside a {@link GearScoreRuleset} — the wiki
 * renders one table per group. Every build maps its gear-score system into these
 * categories.
 *
 * <p>{@link #getCategory() category} is a closed {@code UPPER_SNAKE_CASE} vocabulary:
 * {@code LEVEL} / {@code ATTRIBUTE} / {@code AUGMENT} / {@code ENCHANT_PROFILE} /
 * {@code SET_BONUS} / {@code AURA} / {@code ACHIEVEMENT} / {@code SKILL}.</p>
 */
public final class GearScoreRuleGroup {

    private final String category;
    private final LocalizedText label;
    private final @Nullable LocalizedText description;
    private final List<GearScoreRule> rules;

    public GearScoreRuleGroup(String category,
                              LocalizedText label,
                              @Nullable LocalizedText description,
                              @Nullable List<GearScoreRule> rules) {
        this.category = Objects.requireNonNull(category, "GearScoreRuleGroup.category is required");
        this.label = Objects.requireNonNull(label, "GearScoreRuleGroup.label is required");
        this.description = description;
        this.rules = rules == null
                ? Collections.<GearScoreRule>emptyList()
                : Collections.unmodifiableList(new ArrayList<GearScoreRule>(rules));
    }

    /**
     * Group category — closed {@code UPPER_SNAKE_CASE} vocabulary.
     */
    public String getCategory() {
        return category;
    }

    /**
     * Group heading for the wiki.
     */
    public LocalizedText getLabel() {
        return label;
    }

    /**
     * Optional group description; {@code null} when none supplied.
     */
    public @Nullable LocalizedText getDescription() {
        return description;
    }

    /**
     * Rules in this group; never {@code null} (empty when the group carries none).
     */
    public List<GearScoreRule> getRules() {
        return rules;
    }

    public Builder toBuilder() {
        return new Builder()
                .category(category)
                .label(label)
                .description(description)
                .rules(rules);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GearScoreRuleGroup)) return false;
        GearScoreRuleGroup that = (GearScoreRuleGroup) o;
        return category.equals(that.category)
                && label.equals(that.label)
                && Objects.equals(description, that.description)
                && rules.equals(that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, label, description, rules);
    }

    @Override
    public String toString() {
        return "GearScoreRuleGroup[category=" + category
                + ", rules=" + rules.size() + "]";
    }

    public static final class Builder {
        private @Nullable String category;
        private @Nullable LocalizedText label;
        private @Nullable LocalizedText description;
        private @Nullable List<GearScoreRule> rules;

        public Builder category(String category) {
            this.category = category;
            return this;
        }

        public Builder label(LocalizedText label) {
            this.label = label;
            return this;
        }

        public Builder description(@Nullable LocalizedText description) {
            this.description = description;
            return this;
        }

        public Builder rules(@Nullable List<GearScoreRule> rules) {
            this.rules = rules;
            return this;
        }

        public GearScoreRuleGroup build() {
            return new GearScoreRuleGroup(category, label, description, rules);
        }
    }
}
