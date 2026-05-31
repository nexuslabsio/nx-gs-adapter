package app.l2nx.gs.adapter.api.kafka.events.ratings;

/**
 * Canonical values for {@link RatingSnapshotEvent#getRatingType()}. The field is
 * an <b>open string</b> so a host shipping a new leaderboard is not a breaking
 * contract change. Mirrors the {@code WellKnown*} pattern on the other event
 * DTOs; the set is non-exhaustive and the platform stores unknown rating types
 * verbatim. Adding a constant is a non-breaking minor-version change. Values are
 * {@code lower_snake_case}.
 *
 * <ul>
 *   <li>{@link #FISHING} — the fishing-championship leaderboard (ranked by
 *   accumulated fishing points). The first ranked leaderboard to ship; aligns
 *   with the {@code fishing} bucket key of {@code WellKnownServerOnlineBuckets}.</li>
 * </ul>
 */
public final class WellKnownRatingTypes {

    private WellKnownRatingTypes() {
    }

    public static final String FISHING = "fishing";
}
