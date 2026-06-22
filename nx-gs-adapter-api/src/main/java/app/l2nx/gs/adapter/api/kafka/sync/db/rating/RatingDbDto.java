package app.l2nx.gs.adapter.api.kafka.sync.db.rating;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Build-agnostic wire DTO for one character's standing in a ranked rating, payload of
 * {@code SyncEvent<RatingDbDto>} on the unified db-sync rating topic
 * ({@code <tenant>.gs.sync.db.rating}). One topic carries every rating kind;
 * {@link #getRatingType() ratingType} discriminates which leaderboard the row belongs
 * to. The source is a per-character row in the game DB, replicated via CDC, so rank is
 * NOT on the wire — consumers compute it at read time with a window function.
 *
 * <p>Required fields: {@link #getRatingType() ratingType}, {@link #getCharId() charId}
 * and {@link #getPoints() points}. {@code ratingType} is an open string — canonical
 * values live in {@link WellKnownRatingTypes} ({@code lower_snake_case}); a host
 * shipping a new rating is not a breaking contract change, the platform stores unknown
 * types verbatim.</p>
 *
 * <p>{@link #getSeason() season} is {@code null} for a seasonless rating; otherwise the
 * id of the current period. {@link #getMetadata() metadata} carries optional
 * type-specific extras as a flat {@code String→String} map (e.g. {@code streak_days},
 * {@code last_catch_date} as an ISO string, {@code achievements} as a delimited list) —
 * stringified so this contract stays free of typed timestamp fields.</p>
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

    /**
     * Which leaderboard this row belongs to — open string, canonical values in
     * {@link WellKnownRatingTypes} ({@code lower_snake_case}, e.g. {@code "fishing"}).
     */
    public String getRatingType() {
        return ratingType;
    }

    /**
     * Current period id; {@code null} for a seasonless rating.
     */
    public @Nullable String getSeason() {
        return season;
    }

    /**
     * The ranked character — source primary key (character object id).
     */
    public long getCharId() {
        return charId;
    }

    /**
     * The rating's universal score (higher = better); rank is derived at read time,
     * not stored.
     */
    public long getPoints() {
        return points;
    }

    /**
     * Optional type-specific extras as a flat {@code String→String} map; {@code null}
     * when the rating carries none.
     */
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
