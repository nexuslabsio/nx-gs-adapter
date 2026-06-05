package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One effect a {@link SkillLevel} applies — the build-agnostic projection of a core
 * effect template. {@code name} is the effect handler name (e.g. {@code p_attack},
 * {@code Stun}, {@code HealPercent}); {@code params} are its tuning parameters as a
 * flat string→string map (the host flattens the core's parameter set, value-typed
 * however the core stores it).
 *
 * <p>{@code name} is the non-null identity. Effects are per-level — each skill level
 * carries its own list. The full numeric variation across levels is also captured by
 * the typed {@link SkillLevel} columns ({@code power}, {@code abnormalTimeSec}); this
 * carries the qualitative "which handlers fire".</p>
 */
public final class SkillEffect {

    private final String name;
    private final @Nullable Map<String, String> params;

    private SkillEffect(Builder b) {
        this.name = Objects.requireNonNull(b.name, "name");
        this.params = b.params == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(b.params));
    }

    public String getName() {
        return name;
    }

    /**
     * Effect handler parameters as a flat string-keyed map; {@code null} when the
     * handler takes none.
     */
    public @Nullable Map<String, String> getParams() {
        return params;
    }

    public Builder toBuilder() {
        return new Builder()
                .name(name)
                .params(params);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillEffect)) return false;
        SkillEffect that = (SkillEffect) o;
        return Objects.equals(name, that.name) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, params);
    }

    @Override
    public String toString() {
        return "SkillEffect[name=" + name + ", params=" + params + "]";
    }

    public static final class Builder {
        private String name;
        private @Nullable Map<String, String> params;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder params(@Nullable Map<String, String> params) {
            this.params = params;
            return this;
        }

        public SkillEffect build() {
            return new SkillEffect(this);
        }
    }
}
