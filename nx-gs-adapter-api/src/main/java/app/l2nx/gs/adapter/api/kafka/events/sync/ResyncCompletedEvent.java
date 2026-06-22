package app.l2nx.gs.adapter.api.kafka.events.sync;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Per-entity completion signal of a forced db-sync resync. The single message
 * type of the {@code sync} family ({@code <tenant>.gs.events.sync}). Emitted
 * by the db-sync engine after the first FULLY successful post-invalidation
 * CDC cycle for an entity — no degraded window, zero failed and zero
 * still-pending publishes — once per {@code resyncId} drained into that
 * cycle.
 *
 * <p>Both timestamps are stamped on the adapter clock — the same clock that
 * stamps {@code SyncEvent.timestampEpochMs} on the re-published rows — so
 * the platform sweep compares {@link #getCycleStartedAt() cycleStartedAt}
 * against {@code db_synced_at} without cross-host clock skew: every live row
 * carries {@code db_synced_at >= cycleStartedAt} after the forced cycle,
 * ghost rows keep an older stamp and get swept.</p>
 *
 * <p>Fields (all REQUIRED):
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7. Idempotency / ordering key;
 *   platform extracts {@code occurredAt} from the time-ordered prefix.</li>
 *   <li>{@link #getResyncId() resyncId} — the platform-issued operation id
 *   echoed from the originating resync command.</li>
 *   <li>{@link #getEntityName() entityName} — the completed entity.</li>
 *   <li>{@link #getCycleStartedAt() cycleStartedAt} — when the invalidations
 *   were applied (start of the first cycle that carried them); sweep
 *   cutoff.</li>
 *   <li>{@link #getCompletedAt() completedAt} — when the fully successful
 *   cycle finished publishing.</li>
 * </ul>
 *
 * <p>Emission is retried across cycles (a non-successful cycle defers, the
 * first fully successful one emits — the platform sweep is idempotent against
 * duplicates), but delivery itself is best-effort: the event rides the
 * adapter's bounded events queue, where an overflow drop or a failed send is
 * not retried. A lost completion is covered by the platform operation TTL.
 * Partition key: {@code null} (round-robin — low volume, no ordering
 * need).</p>
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
