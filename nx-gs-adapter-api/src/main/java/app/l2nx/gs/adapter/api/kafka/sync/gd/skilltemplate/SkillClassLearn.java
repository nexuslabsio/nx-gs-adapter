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
 * required to learn, {@code levelUpSp} the SP cost. {@code autoGet} marks skills granted
 * automatically on level-up; {@code learnedByNpc} marks skills taught by a trainer NPC.</p>
 */
public final class SkillClassLearn {

    private final int classId;
    private final @Nullable String className;
    private final @Nullable Integer requiredLevel;
    private final @Nullable Long levelUpSp;
    private final @Nullable Boolean autoGet;
    private final @Nullable Boolean learnedByNpc;

    private SkillClassLearn(Builder b) {
        this.classId = b.classId;
        this.className = b.className;
        this.requiredLevel = b.requiredLevel;
        this.levelUpSp = b.levelUpSp;
        this.autoGet = b.autoGet;
        this.learnedByNpc = b.learnedByNpc;
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
    public @Nullable Long getLevelUpSp() {
        return levelUpSp;
    }

    /**
     * Whether the class receives this skill automatically on reaching {@code requiredLevel}.
     */
    public @Nullable Boolean getAutoGet() {
        return autoGet;
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
                .levelUpSp(levelUpSp)
                .autoGet(autoGet)
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
                && Objects.equals(levelUpSp, that.levelUpSp)
                && Objects.equals(autoGet, that.autoGet)
                && Objects.equals(learnedByNpc, that.learnedByNpc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classId, className, requiredLevel, levelUpSp, autoGet, learnedByNpc);
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
        private @Nullable Long levelUpSp;
        private @Nullable Boolean autoGet;
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

        public Builder levelUpSp(@Nullable Long levelUpSp) {
            this.levelUpSp = levelUpSp;
            return this;
        }

        public Builder autoGet(@Nullable Boolean autoGet) {
            this.autoGet = autoGet;
            return this;
        }

        public Builder learnedByNpc(@Nullable Boolean learnedByNpc) {
            this.learnedByNpc = learnedByNpc;
            return this;
        }

        public SkillClassLearn build() {
            return new SkillClassLearn(this);
        }
    }
}
