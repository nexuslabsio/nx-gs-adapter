package app.l2nx.gs.adapter.api.kafka.commands.sync;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Ack of {@link ResyncRowsCommand}: invalidated counts by entity. Target entity is always present; zero-count cascade
 * entities are OMITTED, so {@code keySet()} is exactly the set a {@code ResyncCompletedEvent} will follow for.
 */
public final class ResyncRowsResult {

    private final Map<String, Integer> invalidatedByEntity;

    public ResyncRowsResult(@Nullable Map<String, Integer> invalidatedByEntity) {
        this.invalidatedByEntity = invalidatedByEntity == null
                ? Collections.<String, Integer>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(invalidatedByEntity));
    }

    /** Target entity first, then cascade children in provider declaration order. */
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
