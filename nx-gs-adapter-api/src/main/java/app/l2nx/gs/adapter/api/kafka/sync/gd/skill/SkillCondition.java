package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One cast precondition of a {@link Skill} — the build-agnostic projection of a
 * core condition node ({@code <cond>} subtrees in L2J-family datapacks): "requires a
 * blunt weapon", "target must be undead", "caster level ≥ N", and so on.
 *
 * <p>{@code type} is the core condition identifier verbatim (the condition class simple
 * name without the {@code Condition} prefix, e.g. {@code PlayerLevel},
 * {@code TargetRaceId}, {@code UsingItemType}) — free-form like effect handler names,
 * NOT a closed enum; forks add their own. {@code params} are the condition's operands
 * flattened to a string map (the host stringifies whatever its core stores); {@code null}
 * when the condition takes none or the host cannot expose them.</p>
 *
 * <p>Logic composites are flattened by the provider: AND-nodes dissolve into the flat
 * list (every entry must hold); OR / NOT nodes ride as {@code LogicOr} / {@code LogicNot}
 * entries whose {@code params} name the nested condition types.</p>
 */
public final class SkillCondition {

    private final String type;
    private final @Nullable Map<String, String> params;

    public SkillCondition(String type,
                          @Nullable Map<String, String> params) {
        this.type = Objects.requireNonNull(type, "type");
        this.params = params == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(params));
    }

    public String getType() {
        return type;
    }

    /**
     * Condition operands as a flat string-keyed map; {@code null} when the condition
     * takes none or the host cannot expose them.
     */
    public @Nullable Map<String, String> getParams() {
        return params;
    }

    public Builder toBuilder() {
        return new Builder()
                .type(type)
                .params(params);
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
