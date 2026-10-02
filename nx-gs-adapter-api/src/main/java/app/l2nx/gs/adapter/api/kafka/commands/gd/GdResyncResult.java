package app.l2nx.gs.adapter.api.kafka.commands.gd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Entity names scheduled for re-snapshot, taken from the live provider registry. Ack is schedule-time only.
 */
public final class GdResyncResult {

    private final List<String> acceptedEntities;

    public GdResyncResult(@Nullable List<String> acceptedEntities) {
        this.acceptedEntities = acceptedEntities == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(acceptedEntities));
    }

    /** Never empty on a real ack; zero active entities replies {@code UNAVAILABLE} instead. */
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
        if (!(o instanceof GdResyncResult)) return false;
        GdResyncResult that = (GdResyncResult) o;
        return acceptedEntities.equals(that.acceptedEntities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acceptedEntities);
    }

    @Override
    public String toString() {
        return "GdResyncResult[acceptedEntities=" + acceptedEntities + "]";
    }

    public static final class Builder {
        private @Nullable List<String> acceptedEntities;

        public Builder acceptedEntities(@Nullable List<String> acceptedEntities) {
            this.acceptedEntities = acceptedEntities;
            return this;
        }

        public GdResyncResult build() {
            return new GdResyncResult(acceptedEntities);
        }
    }
}
