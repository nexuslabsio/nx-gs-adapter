package app.l2nx.gs.adapter.api.kafka.commands.sync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Ack of {@link ResyncEntitiesCommand}: entity names enqueued for invalidation (all declared when {@code entities}
 * was omitted). Enqueue-time only; completion follows via {@code ResyncCompletedEvent}.
 */
public final class ResyncEntitiesResult {

    private final List<String> acceptedEntities;

    public ResyncEntitiesResult(@Nullable List<String> acceptedEntities) {
        this.acceptedEntities = acceptedEntities == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(acceptedEntities));
    }

    /** Never empty on a real ack; zero declared entities replies {@code UNAVAILABLE}. */
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
