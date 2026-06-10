package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One class→skill learn entry of a {@link SkillTemplate} — a playable character class that learns
 * this skill, with the level at which it is acquired and its SP cost. Built by inverting the host's
 * per-class skill trees into a per-skill list, so a skill page can show "which classes learn this".
 *
 * <p>{@code classId} (the host's numeric class id) is the non-null identity. {@code className} is the
 * canonical UPPER_SNAKE class token (the host enum name) for display without a id→name lookup;
 * localized class names are resolved consumer-side. {@code requiredLevel} is the character level
 * required to learn, {@code learnSp} the SP cost. {@code autoLearned} marks skills granted
 * automatically on level-up; {@code learnedByNpc} marks skills taught by a trainer NPC.</p>
 */
public final class SkillClassLearn {

    private final int classId;
    private final @Nullable String className;
    private final @Nullable Integer requiredLevel;
    private final @Nullable Long learnSp;
    private final @Nullable Boolean autoLearned;
    private final @Nullable Boolean learnedByNpc;

    public SkillClassLearn(int classId,
                           @Nullable String className,
                           @Nullable Integer requiredLevel,
                           @Nullable Long learnSp,
                           @Nullable Boolean autoLearned,
                           @Nullable Boolean learnedByNpc) {
        this.classId = classId;
        this.className = className;
        this.requiredLevel = requiredLevel;
        this.learnSp = learnSp;
        this.autoLearned = autoLearned;
        this.learnedByNpc = learnedByNpc;
    }

    public int getClassId() {
        return classId;
    }

    /**
     * Canonical UPPER_SNAKE class token (host class-enum name); {@code null} if not resolvable.
     */
    public @Nullable String getClassName() {
        return className;
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
    public @Nullable Boolean getAutoLearned() {
        return autoLearned;
    }

    /**
     * Whether the skill is taught by a trainer NPC.
     */
    public @Nullable Boolean getLearnedByNpc() {
        return learnedByNpc;
    }

    public Builder toBuilder() {
        return new Builder()
                .classId(classId)
                .className(className)
                .requiredLevel(requiredLevel)
                .learnSp(learnSp)
                .autoLearned(autoLearned)
                .learnedByNpc(learnedByNpc);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillClassLearn)) return false;
        SkillClassLearn that = (SkillClassLearn) o;
        return classId == that.classId
                && Objects.equals(className, that.className)
                && Objects.equals(requiredLevel, that.requiredLevel)
                && Objects.equals(learnSp, that.learnSp)
                && Objects.equals(autoLearned, that.autoLearned)
                && Objects.equals(learnedByNpc, that.learnedByNpc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classId, className, requiredLevel, learnSp, autoLearned, learnedByNpc);
    }

    @Override
    public String toString() {
        return "SkillClassLearn[classId=" + classId + ", className=" + className
                + ", requiredLevel=" + requiredLevel + "]";
    }

    public static final class Builder {
        private int classId;
        private @Nullable String className;
        private @Nullable Integer requiredLevel;
        private @Nullable Long learnSp;
        private @Nullable Boolean autoLearned;
        private @Nullable Boolean learnedByNpc;

        public Builder classId(int classId) {
            this.classId = classId;
            return this;
        }

        public Builder className(@Nullable String className) {
            this.className = className;
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

        public Builder autoLearned(@Nullable Boolean autoLearned) {
            this.autoLearned = autoLearned;
            return this;
        }

        public Builder learnedByNpc(@Nullable Boolean learnedByNpc) {
            this.learnedByNpc = learnedByNpc;
            return this;
        }

        public SkillClassLearn build() {
            return new SkillClassLearn(classId, className, requiredLevel, learnSp, autoLearned,
                    learnedByNpc);
        }
    }
}
