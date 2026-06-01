package app.l2nx.gs.adapter.api.kafka.events.raid;

/**
 * Canonical values for the {@code division} metadata key of the raid-family boss
 * DTOs (see {@link WellKnownBossMetadata#DIVISION}). A division groups bosses for
 * display / routing; the value is an <b>open string</b> with no intensity or
 * ordering implied, so a host with a divergent grouping is not a breaking
 * contract change.
 *
 * <p>Mirrors the {@code WellKnown*} pattern on the other event DTOs
 * ({@code WellKnownBossStatuses}, {@code WellKnownKillerTypes}). The set is
 * non-exhaustive: a host MAY emit a non-canonical division string without an API
 * release, and consumers treat unknown values as opaque (display the raw key,
 * route no behaviour on it). Adding a constant here is a non-breaking
 * minor-version change.</p>
 *
 * <ul>
 *   <li>{@link #PIVOWAR}</li>
 *   <li>{@link #LOWWAR}</li>
 *   <li>{@link #MIDWAR}</li>
 *   <li>{@link #BIGWAR}</li>
 * </ul>
 */
public final class WellKnownBossDivisions {

    private WellKnownBossDivisions() {
    }

    public static final String PIVOWAR = "pivowar";

    public static final String LOWWAR = "lowwar";

    public static final String MIDWAR = "midwar";

    public static final String BIGWAR = "bigwar";
}
