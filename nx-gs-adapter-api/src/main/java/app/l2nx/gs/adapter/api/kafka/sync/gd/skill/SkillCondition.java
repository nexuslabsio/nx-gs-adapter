package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One cast precondition of a {@link Skill}, projected from a core condition node. {@code type} is the core
 * condition name without the {@code Condition} prefix (e.g. {@code PlayerLevel}); free-form, forks add their own.
 * AND-nodes are flattened into the list (all must hold); OR / NOT ride as {@code LogicOr} / {@code LogicNot}
 * entries whose {@code params} name the nested types.
 */
public final class SkillCondition {

    private final String type;
    private final @Nullable Map<String, String> params;

    public SkillCondition(String type, @Nullable Map<String, String> params) {
        this.type = Objects.requireNonNull(type, "type");
        this.params = params == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(params));
    }

    public String getType() {
        return type;
    }

    /** {@code null} when the condition takes none or the host cannot expose them. */
    public @Nullable Map<String, String> getParams() {
        return params;
    }

    public Builder toBuilder() {
        return new Builder().type(type).params(params);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillCondition)) return false;
        SkillCondition that = (SkillCondition) o;
        return Objects.equals(type, that.type) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, params);
    }

    @Override
    public String toString() {
        return "SkillCondition[type=" + type + ", params=" + params + "]";
    }

    public static final class Builder {
        private String type;
        private @Nullable Map<String, String> params;

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder params(@Nullable Map<String, String> params) {
            this.params = params;
            return this;
        }

        public SkillCondition build() {
            return new SkillCondition(type, params);
        }
    }
}
