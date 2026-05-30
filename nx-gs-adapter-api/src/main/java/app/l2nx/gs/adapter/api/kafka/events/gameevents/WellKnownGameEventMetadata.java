package app.l2nx.gs.adapter.api.kafka.events.gameevents;

/**
 * Canonical key/value constants for the {@code metadata} map of
 * {@link GameEventEntry}. The canonical set is intentionally narrow — these are
 * the attributes consumers route UI / behaviour on. Hosts MAY publish arbitrary
 * additional non-canonical keys (open map); consumers treat unknown keys as
 * opaque strings and ignore them.
 *
 * <p>Values are <b>build-agnostic</b>: a host adapter maps its own event
 * vocabulary onto these canonical strings, so a consumer's contract holds
 * regardless of the underlying core or its event engine. Adding a new constant
 * is a non-breaking minor-version change in {@code nx-gs-adapter-api}.</p>
 *
 * <h2>Publication contract</h2>
 *
 * <p>All keys are optional — {@code metadata} is absent ({@code null}) when the
 * host does not classify an event. The only key defined today:</p>
 * <ul>
 *     <li>{@link #EVENT_KIND} = {@link #EVENT_KIND_TVT} — set on a team-vs-team
 *         style mass-PvP event (e.g. the host's "TvT"). Hosts map their own
 *         event types onto this canonical value; consumers route category / icon
 *         on it. Events the host does not classify carry <b>no</b>
 *         {@code event_kind} key.</li>
 * </ul>
 */
public final class WellKnownGameEventMetadata {

    private WellKnownGameEventMetadata() {
    }

    /**
     * Canonical metadata key carrying the build-agnostic kind of a game event.
     * Present only when the host classifies the event; an absent key means
     * "kind not classified".
     */
    public static final String EVENT_KIND = "event_kind";

    /**
     * Canonical {@link #EVENT_KIND} value: a team-vs-team style mass-PvP event.
     * On bohpts this maps from {@code TeamVSTeamEvent} / {@code SoloPvpZoneEvent};
     * other cores map their own equivalents. This is the signal the platform
     * uses to group / badge PvP events on the schedule view.
     */
    public static final String EVENT_KIND_TVT = "tvt";
}
