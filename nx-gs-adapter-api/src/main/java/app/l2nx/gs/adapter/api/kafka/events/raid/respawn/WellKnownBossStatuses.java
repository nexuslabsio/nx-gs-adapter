package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

/**
 * Canonical values for {@link BossRespawnEntry#getStatus()}. The canonical set is
 * intentionally narrow — these are the statuses consumers route behaviour on
 * (boss-timer view: is it up, or counting down to respawn). The field is an open
 * string: hosts MAY emit additional non-canonical statuses (e.g. a dormant /
 * in-combat distinction) and consumers treat unknown values as opaque, mapping
 * them to "not dead" for display.
 *
 * <p>Values are <b>build-agnostic</b>: a host adapter maps its own boss-state
 * vocabulary (L2J {@code ALIVE/DEAD/UNDEFINED}, epic {@code Alive/Wait/Fight/Dead},
 * fork equivalents) onto these canonical strings, so a consumer's contract holds
 * regardless of the underlying core. Adding a new constant is a non-breaking
 * minor-version change in {@code nx-gs-adapter-api}.</p>
 *
 * <ul>
 *     <li>{@link #ALIVE} — the boss is currently up and idle (standing /
 *         killable, no one engaging). No
 *         {@link BossRespawnEntry#getNextRespawnAt() nextRespawnAt}.</li>
 *     <li>{@link #IN_COMBAT} — the boss is up and actively being fought
 *         (engaged / has attackers — "being farmed"). No respawn time. A
 *         refinement of {@link #ALIVE}: consumers that don't model combat treat
 *         it as alive.</li>
 *     <li>{@link #DEAD} — the boss is down and counting toward respawn;
 *         {@link BossRespawnEntry#getNextRespawnAt() nextRespawnAt} carries the
 *         scheduled respawn instant when known.</li>
 * </ul>
 */
public final class WellKnownBossStatuses {

    private WellKnownBossStatuses() {
    }

    /**
     * Boss is up and idle (standing / killable, nobody engaging).
     */
    public static final String ALIVE = "alive";

    /**
     * Boss is up and actively being fought ("being farmed").
     */
    public static final String IN_COMBAT = "in_combat";

    /**
     * Boss is down and waiting to respawn.
     */
    public static final String DEAD = "dead";
}
