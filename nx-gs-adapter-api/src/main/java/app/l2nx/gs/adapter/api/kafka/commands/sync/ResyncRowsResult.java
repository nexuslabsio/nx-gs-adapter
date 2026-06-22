package app.l2nx.gs.adapter.api.kafka.commands.sync;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Success payload of {@link ResyncRowsCommand}. Carries the invalidation
 * counts known at ack time, keyed by entity name: the target entity maps to
 * the number of distinct requested PKs; with {@code cascade=true} each child
 * entity maps to the number of cascade-resolved rows. Entities that resolved
 * zero cascade rows are OMITTED from the map (the target entity is always
 * present). {@code keySet()} therefore enumerates exactly the entities a
 * {@code ResyncCompletedEvent} will follow for.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class ResyncRowsResult {

    private final Map<String, Integer> invalidatedByEntity;

    public ResyncRowsResult(@Nullable Map<String, Integer> invalidatedByEntity) {
        this.invalidatedByEntity = invalidatedByEntity == null
                ? Collections.<String, Integer>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(invalidatedByEntity));
    }

    /**
     * Per-entity invalidated-row counts; zero-count cascade entities omitted.
     * Iteration order: target entity first, cascade children in provider
     * declaration order.
     */
    public Map<String, Integer> getInvalidatedByEntity() {
        return invalidatedByEntity;
    }

    public Builder toBuilder() {
        return new Builder().invalidatedByEntity(invalidatedByEntity);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResyncRowsResult)) return false;
        ResyncRowsResult that = (ResyncRowsResult) o;
        return invalidatedByEntity.equals(that.invalidatedByEntity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(invalidatedByEntity);
    }

    @Override
    public String toString() {
        return "ResyncRowsResult[invalidatedByEntity=" + invalidatedByEntity + "]";
    }

    public static final class Builder {
        private @Nullable Map<String, Integer> invalidatedByEntity;

        public Builder invalidatedByEntity(@Nullable Map<String, Integer> invalidatedByEntity) {
            this.invalidatedByEntity = invalidatedByEntity;
            return this;
        }

        public ResyncRowsResult build() {
            return new ResyncRowsResult(invalidatedByEntity);
        }
    }
}
