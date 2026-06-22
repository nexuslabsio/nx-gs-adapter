package app.l2nx.gs.adapter.api.kafka.events.character;

/**
 * Canonical values for the {@code killer_type} metadata key of
 * {@link CharacterDeathEvent} (see {@link WellKnownDeathMetadata#KILLER_TYPE}).
 * The value is an <b>open string</b> so a core with a divergent death model is not
 * a breaking contract change; every standard L2 death maps onto one of the
 * constants below.
 *
 * <p>Mirrors the {@code WellKnown*} pattern on the other event DTOs
 * ({@code WellKnownSiegeOutcomes}, {@code WellKnownPresenceMetadata}). The set is
 * non-exhaustive: a host MAY emit a non-canonical killer-type string without an
 * API release, and consumers treat unknown values as opaque (display / route no
 * behaviour on it). Adding a constant here is a non-breaking minor-version
 * change. Values are {@code lower_snake_case}.</p>
 *
 * <ul>
 *   <li>{@link #MONSTER} — killed by a non-boss NPC (PvE).</li>
 *   <li>{@link #PLAYER} — killed by another player (PvP).</li>
 *   <li>{@link #BOSS} — killed by a raid / grand boss.</li>
 *   <li>{@link #SELF} — self-inflicted (e.g. suicide skill, fall, environment).</li>
 * </ul>
 */
public final class WellKnownKillerTypes {

    private WellKnownKillerTypes() {}

    public static final String MONSTER = "monster";

    public static final String PLAYER = "player";

    public static final String BOSS = "boss";

    public static final String SELF = "self";
}
