package app.l2nx.gs.adapter.api.kafka.events.castle;

/**
 * Canonical values for {@link SiegeFinishedEvent#getOutcome()}. A siege ends in
 * one of three ways from the castle's point of view; the field is an
 * <b>open string</b> so a core with a divergent siege model is not a breaking
 * contract change, but every standard L2 castle siege maps onto one of the
 * constants below.
 *
 * <p>Mirrors the {@code WellKnown*} pattern on the other event DTOs
 * ({@code WellKnownBossStatuses}, {@code WellKnownGameEventMetadata}). The set is
 * non-exhaustive: a host MAY emit a non-canonical outcome string without an API
 * release, and consumers treat unknown values as opaque (display the raw string,
 * route no behaviour on it). Adding a constant here is a non-breaking
 * minor-version change.</p>
 *
 * <ul>
 *   <li>{@link #CAPTURED} — a clan other than the prior owner took the castle.
 *   {@link SiegeFinishedEvent#getWinnerClanId() winnerClanId} is the captor.</li>
 *   <li>{@link #DEFENDED} — the prior owner held the castle.
 *   {@code winnerClanId} is that defender.</li>
 *   <li>{@link #DRAW} — the castle is unowned at siege end (no clan held it).
 *   {@code winnerClanId} is {@code null}.</li>
 * </ul>
 */
public final class WellKnownSiegeOutcomes {

    private WellKnownSiegeOutcomes() {}

    /**
     * A different clan captured the castle; {@code winnerClanId} is the captor.
     */
    public static final String CAPTURED = "captured";

    /**
     * The prior owner successfully defended; {@code winnerClanId} is that clan.
     */
    public static final String DEFENDED = "defended";

    /**
     * No clan held the castle at siege end; {@code winnerClanId} is {@code null}.
     */
    public static final String DRAW = "draw";
}
