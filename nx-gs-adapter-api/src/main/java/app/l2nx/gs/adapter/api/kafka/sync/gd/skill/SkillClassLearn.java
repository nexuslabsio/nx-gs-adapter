package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A playable class that learns a {@link Skill}, inverted from the host's per-class skill trees.
 * {@code clazz} is the identity; {@code skillLevel} is the skill level this entry grants.
 */
public final class SkillClassLearn {

    private final @Nullable CharacterClass clazz;
    private final @Nullable Integer requiredLevel;
    private final @Nullable Long learnSp;
    private final @Nullable Boolean autoLearn;
    private final @Nullable Boolean learnedByNpc;
    private final @Nullable Integer skillLevel;
    private final @Nullable List<SkillLearnItem> requiredItems;

    public SkillClassLearn(
            @Nullable CharacterClass clazz,
            @Nullable Integer requiredLevel,
            @Nullable Long learnSp,
            @Nullable Boolean autoLearn,
            @Nullable Boolean learnedByNpc,
            @Nullable Integer skillLevel,
            @Nullable List<SkillLearnItem> requiredItems) {
        this.clazz = clazz;
        this.requiredLevel = requiredLevel;
        this.learnSp = learnSp;
        this.autoLearn = autoLearn;
        this.learnedByNpc = learnedByNpc;
        this.skillLevel = skillLevel;
        this.requiredItems = requiredItems == null
                ? null
                : Collections.unmodifiableList(new ArrayList<SkillLearnItem>(requiredItems));
    }

    public @Nullable CharacterClass getClazz() {
        return clazz;
    }

    public @Nullable Integer getRequiredLevel() {
        return requiredLevel;
    }

    public @Nullable Long getLearnSp() {
        return learnSp;
    }

    /** Granted automatically on reaching {@code requiredLevel}. */
    public @Nullable Boolean getAutoLearn() {
        return autoLearn;
    }

    public @Nullable Boolean getLearnedByNpc() {
        return learnedByNpc;
    }

    public @Nullable Integer getSkillLevel() {
        return skillLevel;
    }

    /** {@code null} or empty means no item cost. */
    public @Nullable List<SkillLearnItem> getRequiredItems() {
        return requiredItems;
    }

    public Builder toBuilder() {
        return new Builder()
                .clazz(clazz)
                .requiredLevel(requiredLevel)
                .learnSp(learnSp)
                .autoLearn(autoLearn)
                .learnedByNpc(learnedByNpc)
                .skillLevel(skillLevel)
                .requiredItems(requiredItems);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillClassLearn)) return false;
        SkillClassLearn that = (SkillClassLearn) o;
        return clazz == that.clazz
                && Objects.equals(requiredLevel, that.requiredLevel)
                && Objects.equals(learnSp, that.learnSp)
                && Objects.equals(autoLearn, that.autoLearn)
                && Objects.equals(learnedByNpc, that.learnedByNpc)
                && Objects.equals(skillLevel, that.skillLevel)
                && Objects.equals(requiredItems, that.requiredItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, requiredLevel, learnSp, autoLearn, learnedByNpc, skillLevel, requiredItems);
    }

    @Override
    public String toString() {
        return "SkillClassLearn[clazz=" + clazz + ", requiredLevel=" + requiredLevel + "]";
    }

    public static final class Builder {
        private @Nullable CharacterClass clazz;
        private @Nullable Integer requiredLevel;
        private @Nullable Long learnSp;
        private @Nullable Boolean autoLearn;
        private @Nullable Boolean learnedByNpc;
        private @Nullable Integer skillLevel;
        private @Nullable List<SkillLearnItem> requiredItems;

        public Builder clazz(@Nullable CharacterClass clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder requiredLevel(@Nullable Integer requiredLevel) {
            this.requiredLevel = requiredLevel;
            return this;
        }

        public Builder learnSp(@Nullable Long learnSp) {
            this.learnSp = learnSp;
            return this;
        }

        public Builder autoLearn(@Nullable Boolean autoLearn) {
            this.autoLearn = autoLearn;
            return this;
        }

        public Builder learnedByNpc(@Nullable Boolean learnedByNpc) {
            this.learnedByNpc = learnedByNpc;
            return this;
        }

        public Builder skillLevel(@Nullable Integer skillLevel) {
            this.skillLevel = skillLevel;
            return this;
        }

        public Builder requiredItems(@Nullable List<SkillLearnItem> requiredItems) {
            this.requiredItems = requiredItems;
            return this;
        }

        public SkillClassLearn build() {
            return new SkillClassLearn(
                    clazz, requiredLevel, learnSp, autoLearn, learnedByNpc, skillLevel, requiredItems);
        }
    }
}
