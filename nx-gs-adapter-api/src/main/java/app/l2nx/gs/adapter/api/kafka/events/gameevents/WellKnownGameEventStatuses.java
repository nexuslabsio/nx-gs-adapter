package app.l2nx.gs.adapter.api.kafka.events.gameevents;

/**
 * Canonical values for {@link GameEventEntry#getStatus()}. The canonical set is
 * the event lifecycle the consumer routes its schedule badge on. The field is an
 * open string: hosts MAY emit additional non-canonical phases and consumers treat
 * unknown values as opaque, mapping them to {@link #WAITING} for display.
 *
 * <p>Values are <b>build-agnostic</b>: a host adapter maps its own event-engine
 * state machine (bohpts {@code EVENT_STATE = NOT_ACTIVE / COUNT_DOWN / PREPARATION
 * / STARTED / OVER}, fork equivalents) onto these canonical strings, so a
 * consumer's contract holds regardless of the underlying core. Adding a new
 * constant is a non-breaking minor-version change in {@code nx-gs-adapter-api}.</p>
 *
 * <ul>
 *     <li>{@link #WAITING} — no occurrence is active; the event is idle until its
 *         next scheduled start.</li>
 *     <li>{@link #REGISTRATION} — the event is in its pre-start registration /
 *         preparation phase (players can sign up / a countdown is running).</li>
 *     <li>{@link #IN_PROGRESS} — the event is currently running.</li>
 * </ul>
 */
public final class WellKnownGameEventStatuses {

    private WellKnownGameEventStatuses() {}

    /**
     * No occurrence active; idle until the next scheduled start.
     */
    public static final String WAITING = "waiting";

    /**
     * Pre-start registration / preparation phase.
     */
    public static final String REGISTRATION = "registration";

    /**
     * Event currently running.
     */
    public static final String IN_PROGRESS = "in_progress";
}
