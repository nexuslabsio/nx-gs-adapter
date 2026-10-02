package app.l2nx.gs.adapter.api.kafka.sync.db.rating;

/**
 * Canonical, non-exhaustive values for {@link RatingDbDto#getRatingType()}; unknown types are stored verbatim.
 * {@link #FISHING} aligns with the {@code fishing} key of {@code WellKnownServerOnlineBuckets}.
 */
public final class WellKnownRatingTypes {

    private WellKnownRatingTypes() {}

    public static final String FISHING = "fishing";
}
