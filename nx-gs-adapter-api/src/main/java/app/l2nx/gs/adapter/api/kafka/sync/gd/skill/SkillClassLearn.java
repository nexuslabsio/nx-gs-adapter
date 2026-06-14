package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One class→skill learn entry of a {@link Skill} — a playable class that learns this skill,
 * with the level at which it is acquired and its SP cost. Built by inverting the host's per-class
 * skill trees into a per-skill list, so a skill page can show "which classes learn this".
 *
 * <p>{@code clazz} (the canonical {@link CharacterClass} token) is the class identity — no
 * source-side numeric id. {@code requiredLevel} is the character level required to learn,
 * {@code learnSp} the SP cost, {@code skillLevel} the skill's own level granted by this entry.
 * {@code autoLearn} marks skills granted automatically on level-up; {@code learnedByNpc} marks
 * skills taught by a trainer NPC. {@code requiredItems} lists the items consumed to learn the
 * skill at this entry.</p>
 */
public final class SkillClassLearn {

    private final @Nullable CharacterClass clazz;
    private final @Nullable Integer requiredLevel;
    private final @Nullable Long learnSp;
    private final @Nullable Boolean autoLearn;
    private final @Nullable Boolean learnedByNpc;
    private final @Nullable Integer skillLevel;
    private final @Nullable List<SkillLearnItem> requiredItems;

    public SkillClassLearn(@Nullable CharacterClass clazz,
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

    /**
     * Canonical class that learns the skill; non-null for a playable class.
     */
    public @Nullable CharacterClass getClazz() {
        return clazz;
    }

    /**
     * Character level required to learn the skill at this entry.
     */
    public @Nullable Integer getRequiredLevel() {
        return requiredLevel;
    }

    /**
     * SP cost to learn the skill at this entry.
     */
    public @Nullable Long getLearnSp() {
        return learnSp;
    }

    /**
     * Whether the class receives this skill automatically on reaching {@code requiredLevel}.
     */
    public @Nullable Boolean getAutoLearn() {
        return autoLearn;
    }

    /**
     * Whether the skill is taught by a trainer NPC.
     */
    public @Nullable Boolean getLearnedByNpc() {
        return learnedByNpc;
    }

    /**
     * The skill level this acquisition entry grants (the skill's own level, e.g. Lv.2).
     */
    public @Nullable Integer getSkillLevel() {
        return skillLevel;
    }

    /**
     * Items consumed to learn the skill at this entry; {@code null}/empty = no item cost.
     */
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
            return new SkillClassLearn(clazz, requiredLevel, learnSp, autoLearn, learnedByNpc, skillLevel, requiredItems);
        }
    }
}
