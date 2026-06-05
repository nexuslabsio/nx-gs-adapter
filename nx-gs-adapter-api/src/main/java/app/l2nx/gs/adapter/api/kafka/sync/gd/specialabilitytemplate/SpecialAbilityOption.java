package app.l2nx.gs.adapter.api.kafka.sync.gd.specialabilitytemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One special-ability option a {@link SpecialAbilityTemplate} stone offers — the ensoul
 * choice and the skill it grants. {@code optionId} is the non-null identity; the option
 * carries no display text of its own, so the consumer resolves a name by joining
 * {@code skillTemplateId} to the skill-template entity.
 */
public final class SpecialAbilityOption {

    private final int optionId;
    private final @Nullable Integer skillTemplateId;
    private final @Nullable Integer skillLevel;

    public SpecialAbilityOption(int optionId,
                                @Nullable Integer skillTemplateId,
                                @Nullable Integer skillLevel) {
        this.optionId = optionId;
        this.skillTemplateId = skillTemplateId;
        this.skillLevel = skillLevel;
    }

    public int getOptionId() {
        return optionId;
    }

    public @Nullable Integer getSkillTemplateId() {
        return skillTemplateId;
    }

    public @Nullable Integer getSkillLevel() {
        return skillLevel;
    }

    public Builder toBuilder() {
        return new Builder()
                .optionId(optionId)
                .skillTemplateId(skillTemplateId)
                .skillLevel(skillLevel);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpecialAbilityOption)) return false;
        SpecialAbilityOption that = (SpecialAbilityOption) o;
        return optionId == that.optionId
                && Objects.equals(skillTemplateId, that.skillTemplateId)
                && Objects.equals(skillLevel, that.skillLevel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(optionId, skillTemplateId, skillLevel);
    }

    @Override
    public String toString() {
        return "SpecialAbilityOption[optionId=" + optionId + ", skillTemplateId=" + skillTemplateId
                + ", skillLevel=" + skillLevel + "]";
    }

    public static final class Builder {
        private int optionId;
        private @Nullable Integer skillTemplateId;
        private @Nullable Integer skillLevel;

        public Builder optionId(int optionId) {
            this.optionId = optionId;
            return this;
        }

        public Builder skillTemplateId(@Nullable Integer skillTemplateId) {
            this.skillTemplateId = skillTemplateId;
            return this;
        }

        public Builder skillLevel(@Nullable Integer skillLevel) {
            this.skillLevel = skillLevel;
            return this;
        }

        public SpecialAbilityOption build() {
            return new SpecialAbilityOption(optionId, skillTemplateId, skillLevel);
        }
    }
}
