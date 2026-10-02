package app.l2nx.gs.adapter.api.kafka.events.sync;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Completion signal of a forced db-sync resync for one entity; the only message type of the {@code sync}
 * family. Emitted once per drained {@code resyncId} after the first fully successful post-invalidation CDC
 * cycle (no degraded window, zero failed or pending publishes).
 *
 * <p>Both timestamps use the adapter clock, the same one stamping {@code SyncEvent.timestampEpochMs}, so the
 * platform sweep can compare {@code cycleStartedAt} to {@code db_synced_at} without clock skew: live rows end
 * up with {@code db_synced_at >= cycleStartedAt}, ghost rows keep an older stamp and are swept.</p>
 *
 * <p>Emission is retried across cycles (the sweep is idempotent), but delivery is best-effort: an overflow
 * drop or failed send on the bounded events queue is not retried, and a lost completion is covered by the
 * platform operation TTL. Partition key is {@code null} (round-robin).</p>
 */
public final class ResyncCompletedEvent {

    private final UUID eventId;
    private final UUID resyncId;
    private final String entityName;
    private final Instant cycleStartedAt;
    private final Instant completedAt;

    public ResyncCompletedEvent(
            UUID eventId, UUID resyncId, String entityName, Instant cycleStartedAt, Instant completedAt) {
        this.eventId = Objects.requireNonNull(eventId, "ResyncCompletedEvent.eventId is required");
        this.resyncId = Objects.requireNonNull(resyncId, "ResyncCompletedEvent.resyncId is required");
        this.entityName = Objects.requireNonNull(entityName, "ResyncCompletedEvent.entityName is required");
        this.cycleStartedAt = Objects.requireNonNull(cycleStartedAt, "ResyncCompletedEvent.cycleStartedAt is required");
        this.completedAt = Objects.requireNonNull(completedAt, "ResyncCompletedEvent.completedAt is required");
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getResyncId() {
        return resyncId;
    }

    public String getEntityName() {
        return entityName;
    }

    public Instant getCycleStartedAt() {
        return cycleStartedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .resyncId(resyncId)
                .entityName(entityName)
                .cycleStartedAt(cycleStartedAt)
                .completedAt(completedAt);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResyncCompletedEvent)) return false;
        ResyncCompletedEvent that = (ResyncCompletedEvent) o;
        return eventId.equals(that.eventId)
                && resyncId.equals(that.resyncId)
                && entityName.equals(that.entityName)
                && cycleStartedAt.equals(that.cycleStartedAt)
                && completedAt.equals(that.completedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, resyncId, entityName, cycleStartedAt, completedAt);
    }

    @Override
    public String toString() {
        return "ResyncCompletedEvent[eventId=" + eventId
                + ", resyncId=" + resyncId
                + ", entityName=" + entityName
                + ", cycleStartedAt=" + cycleStartedAt
                + ", completedAt=" + completedAt + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private @Nullable UUID resyncId;
        private @Nullable String entityName;
        private @Nullable Instant cycleStartedAt;
        private @Nullable Instant completedAt;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder resyncId(UUID resyncId) {
            this.resyncId = resyncId;
            return this;
        }

        public Builder entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder cycleStartedAt(Instant cycleStartedAt) {
            this.cycleStartedAt = cycleStartedAt;
            return this;
        }

        public Builder completedAt(Instant completedAt) {
            this.completedAt = completedAt;
            return this;
        }

        public ResyncCompletedEvent build() {
            return new ResyncCompletedEvent(eventId, resyncId, entityName, cycleStartedAt, completedAt);
        }
    }
}
