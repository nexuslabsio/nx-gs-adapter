package app.l2nx.gs.adapter.api.kafka.commands.gd;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Success payload of {@link GdResyncCommand}. Carries the names of every
 * gd-sync entity scheduled for re-snapshot — the full registered set
 * (itemtemplate, npctemplate, skill, recipe, armorset, soulcrystal, class,
 * instance, …), taken from the live provider registry rather than hardcoded.
 * The ack is schedule-time only; per-entity completion follows asynchronously
 * via the nx-gamedata {@code SNAPSHOT_COMPLETE} markers.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class GdResyncResult {

    private final List<String> acceptedEntities;

    public GdResyncResult(@Nullable List<String> acceptedEntities) {
        this.acceptedEntities = acceptedEntities == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(acceptedEntities));
    }

    /**
     * Entity names scheduled for re-snapshot. Never empty on a real ack — an
     * adapter with zero active gd entities replies {@code UNAVAILABLE} instead.
     */
    public List<String> getAcceptedEntities() {
        return acceptedEntities;
    }

    public Builder toBuilder() {
        return new Builder()
                .acceptedEntities(acceptedEntities);
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
