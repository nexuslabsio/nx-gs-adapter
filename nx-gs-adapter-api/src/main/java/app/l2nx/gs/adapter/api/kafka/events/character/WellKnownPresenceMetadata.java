package app.l2nx.gs.adapter.api.kafka.events.character;

/**
 * Canonical key/value constants for the {@code metadata} map of
 * {@link CharacterPresenceEvent}. The canonical set is intentionally narrow —
 * these are the attributes consumers route behaviour on. Hosts MAY publish
 * arbitrary additional non-canonical keys (open map); consumers treat unknown
 * keys as opaque strings and ignore them.
 *
 * <p>Values are <b>build-agnostic</b>: a host adapter (L2J / Lucera / Essence
 * forks) maps its own teardown / packet vocabulary onto these canonical
 * strings, so a consumer's contract holds regardless of the underlying core.
 * Adding a new constant is a non-breaking minor-version change in
 * {@code nx-gs-adapter-api}.</p>
 *
 * <h2>Publication contract</h2>
 *
 * <p>All keys are optional — {@code metadata} is absent ({@code null}) on the
 * common path (e.g. login events, voluntary logout). The only key defined today:</p>
 * <ul>
 *     <li>{@link #LOGOUT_REASON} = {@link #LOGOUT_REASON_DISCONNECT} — set on a
 *         logout event ({@code online=false}) that was caused by an
 *         <em>involuntary</em> connection loss (network drop, client crash,
 *         AFK / anti-bot kick, DDoS). Voluntary exit ("return to character
 *         select" / "exit game") and server-initiated logout (admin kick,
 *         maintenance, idle timeout) carry <b>no</b> {@code logout_reason}
 *         key. Consumers MAY notify the player on this value.</li>
 * </ul>
 */
public final class WellKnownPresenceMetadata {

    private WellKnownPresenceMetadata() {
    }

    /**
     * Canonical metadata key carrying the build-agnostic reason a character
     * left the world. Present only on logout events and only for the reasons
     * the host chooses to classify (today: involuntary disconnect). Absent
     * key means "reason not classified".
     */
    public static final String LOGOUT_REASON = "logout_reason";

    /**
     * Canonical {@link #LOGOUT_REASON} value: the character left the world by
     * an involuntary connection loss — network drop, client crash, AFK /
     * anti-bot kick, or DDoS — rather than a deliberate logout. This is the
     * signal that powers the "your fishing character got disconnected"
     * Telegram notification.
     */
    public static final String LOGOUT_REASON_DISCONNECT = "disconnect";
}
