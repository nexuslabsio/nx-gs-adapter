package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

/**
 * Canonical values for {@link BossRespawnEntry#getStatus()}. The field is an open string: hosts map their
 * own boss-state vocabulary onto these, MAY emit others, and consumers treat unknown values as "not dead".
 *
 * <ul>
 *     <li>{@link #ALIVE} - up and idle; no {@code nextRespawnAt}.</li>
 *     <li>{@link #IN_COMBAT} - up and being fought; consumers that do not model combat treat it as alive.</li>
 *     <li>{@link #DEAD} - counting toward respawn; {@code nextRespawnAt} set when known.</li>
 * </ul>
 */
public final class WellKnownBossStatuses {

    private WellKnownBossStatuses() {}

    public static final String ALIVE = "alive";

    public static final String IN_COMBAT = "in_combat";

    public static final String DEAD = "dead";
}
