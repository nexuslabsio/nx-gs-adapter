package app.l2nx.gs.adapter.api.kafka.events.ratings;

import java.util.Objects;

/**
 * One ranked row of a {@link RatingSnapshotEvent}. Carries the character id, the
 * leaderboard score, and the host-computed rank. Character <b>name is not on the
 * wire</b> — the platform joins the character catalog (e.g. {@code gs_characters})
 * to resolve a current display name, so renames are reflected without a fresh
 * snapshot.
 *
 * <ul>
 *   <li>{@link #getCharId() charId} — the ranked character.</li>
 *   <li>{@link #getScore() score} — the leaderboard score in the rating's own
 *   units (e.g. fishing championship points). Host-defined semantics per
 *   {@code ratingType}.</li>
 *   <li>{@link #getRank() rank} — 1-based rank within this snapshot, computed by
 *   the host (it owns the ordering semantics for the rating type).</li>
 * </ul>
 */
public final class RatingEntry {

    private final long charId;
    private final long score;
    private final int rank;

    public RatingEntry(long charId, long score, int rank) {
        this.charId = charId;
        this.score = score;
        this.rank = rank;
    }

    public long getCharId() {
        return charId;
    }

    public long getScore() {
        return score;
    }

    public int getRank() {
        return rank;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .score(score)
                .rank(rank);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RatingEntry)) return false;
        RatingEntry that = (RatingEntry) o;
        return charId == that.charId && score == that.score && rank == that.rank;
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, score, rank);
    }

    @Override
    public String toString() {
        return "RatingEntry[charId=" + charId + ", score=" + score + ", rank=" + rank + "]";
    }

    public static final class Builder {
        private long charId;
        private long score;
        private int rank;

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder score(long score) {
            this.score = score;
            return this;
        }

        public Builder rank(int rank) {
            this.rank = rank;
            return this;
        }

        public RatingEntry build() {
            return new RatingEntry(charId, score, rank);
        }
    }
}
