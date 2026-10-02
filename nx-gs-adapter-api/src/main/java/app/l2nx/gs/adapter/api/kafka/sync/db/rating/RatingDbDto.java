package app.l2nx.gs.adapter.api.kafka.sync.db.rating;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one character's standing in a ranked rating, payload of {@code SyncEvent<RatingDbDto>}.
 * {@code ratingType} discriminates the leaderboard; rank is not on the wire, consumers derive it at read time.
 *
 * {@code ratingType} is an open string (canonical values in {@link WellKnownRatingTypes}); unknown types are stored verbatim.
 */
public final class RatingDbDto {

    private final String ratingType;
    private final @Nullable String season;
    private final long charId;
    private final long points;
    private final @Nullable Map<String, String> metadata;

    public RatingDbDto(
            String ratingType,
            @Nullable String season,
            long charId,
            long points,
            @Nullable Map<String, String> metadata) {
        this.ratingType = Objects.requireNonNull(ratingType, "RatingDbDto.ratingType is required");
        this.season = season;
        this.charId = charId;
        this.points = points;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public String getRatingType() {
        return ratingType;
    }

    /** Null for a seasonless rating. */
    public @Nullable String getSeason() {
        return season;
    }

    public long getCharId() {
        return charId;
    }

    /** Higher is better. */
    public long getPoints() {
        return points;
    }

    /** Flat String-to-String map of type-specific extras (stringified to avoid typed timestamps); null when none. */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .ratingType(ratingType)
                .season(season)
                .charId(charId)
                .points(points)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RatingDbDto)) return false;
        RatingDbDto that = (RatingDbDto) o;
        return charId == that.charId
                && points == that.points
                && ratingType.equals(that.ratingType)
                && Objects.equals(season, that.season)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratingType, season, charId, points, metadata);
    }

    @Override
    public String toString() {
        return "RatingDbDto[ratingType=" + ratingType
                + ", season=" + season
                + ", charId=" + charId
                + ", points=" + points
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable String ratingType;
        private @Nullable String season;
        private long charId;
        private long points;
        private @Nullable Map<String, String> metadata;

        public Builder ratingType(String ratingType) {
            this.ratingType = ratingType;
            return this;
        }

        public Builder season(@Nullable String season) {
            this.season = season;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder points(long points) {
            this.points = points;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public RatingDbDto build() {
            return new RatingDbDto(ratingType, season, charId, points, metadata);
        }
    }
}
