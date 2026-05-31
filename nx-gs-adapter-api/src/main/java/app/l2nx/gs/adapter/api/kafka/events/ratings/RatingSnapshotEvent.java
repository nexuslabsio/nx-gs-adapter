package app.l2nx.gs.adapter.api.kafka.events.ratings;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Periodic FULL snapshot of one ranked leaderboard. The single message type of
 * the {@code ratings} family ({@code <tenant>.gs.events.ratings}). Host-pushed on
 * a host-managed cadence (bohpts: every 1 minute, fishing championship top-1000).
 *
 * <p>One family carries every leaderboard kind: {@link #getRatingType() ratingType}
 * discriminates which one this snapshot is for (e.g. {@code "fishing"}). The
 * platform stores all kinds in one table keyed by {@code ratingType}, so adding a
 * new leaderboard needs no new family / topic / consumer — only a new
 * {@link WellKnownRatingTypes} constant.</p>
 *
 * <p>Snapshot semantics: each snapshot is the complete current ranking for its
 * {@code ratingType}; the platform scope-replaces the prior snapshot for that
 * {@code (server, ratingType)}, gated by the UUIDv7 {@code eventId} timestamp so
 * a stale / replayed snapshot never regresses the stored ranking.</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency / ordering
 *   key; platform extracts {@code occurredAt} from the time-ordered prefix.</li>
 *   <li>{@link #getRatingType() ratingType} — REQUIRED. Open string identifying
 *   the leaderboard; canonical values in {@link WellKnownRatingTypes}.</li>
 *   <li>{@link #getEntries() entries} — the ranked rows, host-ordered by rank.
 *   Null at the constructor normalizes to an empty list (empty leaderboard);
 *   getter returns an unmodifiable view.</li>
 * </ul>
 *
 * <p>Partition key: {@code null} (round-robin); ordering per server via the
 * UUIDv7 {@code eventId} timestamp, consumers group by the {@code Nx-Server-Id}
 * header.</p>
 */
public final class RatingSnapshotEvent {

    private final UUID eventId;
    private final String ratingType;
    private final List<RatingEntry> entries;

    public RatingSnapshotEvent(UUID eventId,
                               String ratingType,
                               @Nullable List<RatingEntry> entries) {
        this.eventId = Objects.requireNonNull(eventId, "RatingSnapshotEvent.eventId is required");
        this.ratingType = Objects.requireNonNull(ratingType, "RatingSnapshotEvent.ratingType is required");
        this.entries = entries == null
                ? Collections.<RatingEntry>emptyList()
                : Collections.unmodifiableList(new ArrayList<RatingEntry>(entries));
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getRatingType() {
        return ratingType;
    }

    public List<RatingEntry> getEntries() {
        return entries;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .ratingType(ratingType)
                .entries(entries);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RatingSnapshotEvent)) return false;
        RatingSnapshotEvent that = (RatingSnapshotEvent) o;
        return eventId.equals(that.eventId)
                && ratingType.equals(that.ratingType)
                && entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, ratingType, entries);
    }

    @Override
    public String toString() {
        return "RatingSnapshotEvent[eventId=" + eventId
                + ", ratingType=" + ratingType
                + ", entries=" + entries.size() + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private @Nullable String ratingType;
        private @Nullable List<RatingEntry> entries;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder ratingType(String ratingType) {
            this.ratingType = ratingType;
            return this;
        }

        public Builder entries(@Nullable List<RatingEntry> entries) {
            this.entries = entries;
            return this;
        }

        public RatingSnapshotEvent build() {
            return new RatingSnapshotEvent(eventId, ratingType, entries);
        }
    }
}
