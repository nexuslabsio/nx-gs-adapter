package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One gear-score contribution a skill grants; a skill may carry several (e.g. an owning bonus plus a per-level one).
 * {@link #getClassIds() classIds} restricts it to specific classes; {@code null} means every class.
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

    /** {@code OWNED} (flat), {@code PER_LEVEL} (times skill level) or {@code ENCHANT} (times enchant step). */
    public String getKind() {
        return kind;
    }

    /** Points in the unit of {@link #getKind() kind}. */
    public int getValue() {
        return value;
    }

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
