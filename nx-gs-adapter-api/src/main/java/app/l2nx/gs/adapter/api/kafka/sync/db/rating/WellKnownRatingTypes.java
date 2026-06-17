package app.l2nx.gs.adapter.api.kafka.sync.db.rating;

/**
 * Canonical values for {@link RatingDbDto#getRatingType()}. The field is an
 * <b>open string</b> so a host shipping a new rating is not a breaking contract
 * change. Mirrors the {@code WellKnown*} pattern on the other DTOs; the set is
 * non-exhaustive and the platform stores unknown rating types verbatim. Adding a
 * constant is a non-breaking minor-version change. Values are
 * {@code lower_snake_case}.
 *
 * <ul>
 *   <li>{@link #FISHING} — the fishing-championship rating (ranked by accumulated
 *   fishing points). The first rating to ship; aligns with the {@code fishing}
 *   bucket key of {@code WellKnownServerOnlineBuckets}.</li>
 * </ul>
 */
public final class WellKnownRatingTypes {

    private WellKnownRatingTypes() {
    }

    public static final String FISHING = "fishing";
}
