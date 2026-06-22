package app.l2nx.gs.adapter.api.kafka.commands.sync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Success payload of {@link ResyncEntitiesCommand}. Carries the entity names
 * actually enqueued for invalidation — the full declared set when the command
 * omitted {@code entities}, the validated requested set otherwise. The ack is
 * enqueue-time only: per-entity completion follows asynchronously via
 * {@code ResyncCompletedEvent}.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class ResyncEntitiesResult {

    private final List<String> acceptedEntities;

    public ResyncEntitiesResult(@Nullable List<String> acceptedEntities) {
        this.acceptedEntities = acceptedEntities == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(acceptedEntities));
    }

    /**
     * Entity names enqueued for invalidation. Never empty on a real ack —
     * an adapter with zero declared entities replies {@code UNAVAILABLE}
     * instead of an empty accept.
     */
    public List<String> getAcceptedEntities() {
        return acceptedEntities;
    }

    public Builder toBuilder() {
        return new Builder().acceptedEntities(acceptedEntities);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResyncEntitiesResult)) return false;
        ResyncEntitiesResult that = (ResyncEntitiesResult) o;
        return acceptedEntities.equals(that.acceptedEntities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acceptedEntities);
    }

    @Override
    public String toString() {
        return "ResyncEntitiesResult[acceptedEntities=" + acceptedEntities + "]";
    }

    public static final class Builder {
        private @Nullable List<String> acceptedEntities;

        public Builder acceptedEntities(@Nullable List<String> acceptedEntities) {
            this.acceptedEntities = acceptedEntities;
            return this;
        }

        public ResyncEntitiesResult build() {
            return new ResyncEntitiesResult(acceptedEntities);
        }
    }
}
