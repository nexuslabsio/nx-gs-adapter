package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One gear-score contribution a skill grants — a sub-DTO of {@link Skill}. A skill
 * may carry several contributions (e.g. a flat bonus for owning it plus a per-level
 * bonus), so {@link Skill#getGearScoreContributions()} is a list.
 *
 * <p>{@link #getKind() kind} is a closed {@code UPPER_SNAKE_CASE} vocabulary that
 * selects how {@link #getValue() value} scales:</p>
 * <ul>
 *   <li>{@code OWNED} — flat bonus for the character merely having the skill;
 *   {@code value} applied once.</li>
 *   <li>{@code PER_LEVEL} — {@code value} multiplied by the skill's level.</li>
 *   <li>{@code ENCHANT} — {@code value} multiplied by the skill's enchant step;
 *   frequently class-bound.</li>
 * </ul>
 *
 * <p>{@link #getClassIds() classIds} restricts the contribution to specific
 * playable classes — {@code null} means it applies to every class.</p>
 */
public final class GearScoreContribution {

    private final String kind;
    private final int value;
    private final @Nullable List<Integer> classIds;

    public GearScoreContribution(String kind, int value, @Nullable List<Integer> classIds) {
        this.kind = Objects.requireNonNull(kind, "GearScoreContribution.kind is required");
        this.value = value;
        this.classIds = classIds == null ? null : Collections.unmodifiableList(new ArrayList<Integer>(classIds));
    }

    /**
     * Scaling kind — closed {@code UPPER_SNAKE_CASE} vocabulary
     * ({@code OWNED} / {@code PER_LEVEL} / {@code ENCHANT}).
     */
    public String getKind() {
        return kind;
    }

    /**
     * Gear-score points per the {@link #getKind() kind}'s unit (flat for
     * {@code OWNED}, per level for {@code PER_LEVEL}, per enchant step for
     * {@code ENCHANT}).
     */
    public int getValue() {
        return value;
    }

    /**
     * Classes this contribution applies to; {@code null} means all classes.
     */
    public @Nullable List<Integer> getClassIds() {
        return classIds;
    }

    public Builder toBuilder() {
        return new Builder().kind(kind).value(value).classIds(classIds);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GearScoreContribution)) return false;
        GearScoreContribution that = (GearScoreContribution) o;
        return value == that.value && kind.equals(that.kind) && Objects.equals(classIds, that.classIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, value, classIds);
    }

    @Override
    public String toString() {
        return "GearScoreContribution[kind=" + kind + ", value=" + value + ", classIds=" + classIds + "]";
    }

    public static final class Builder {
        private @Nullable String kind;
        private int value;
        private @Nullable List<Integer> classIds;

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public Builder value(int value) {
            this.value = value;
            return this;
        }

        public Builder classIds(@Nullable List<Integer> classIds) {
            this.classIds = classIds;
            return this;
        }

        public GearScoreContribution build() {
            return new GearScoreContribution(kind, value, classIds);
        }
    }
}
