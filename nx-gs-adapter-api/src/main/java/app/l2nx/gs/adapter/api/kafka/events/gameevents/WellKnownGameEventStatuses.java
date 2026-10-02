package app.l2nx.gs.adapter.api.kafka.events.gameevents;

/**
 * Values of {@link GameEventEntry#getStatus()}. Open string: hosts MAY emit other phases, and consumers map unknown
 * values to {@link #WAITING} for display. Host adapters map their own event state machine onto these.
 */
public final class WellKnownGameEventStatuses {

    private WellKnownGameEventStatuses() {}

    public static final String WAITING = "waiting";

    public static final String REGISTRATION = "registration";

    public static final String IN_PROGRESS = "in_progress";
}
