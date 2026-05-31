package app.l2nx.gs.adapter.api.kafka.events.serveronline;

/**
 * Canonical key constants for the {@code metadata} map of
 * {@link ServerStartedEvent}. The map is open — hosts MAY publish arbitrary
 * additional keys; consumers treat unknown keys as opaque. Adding a constant
 * here is a non-breaking minor-version change.
 *
 * <ul>
 *   <li>{@link #GM_ONLY} — {@code "true"} / {@code "false"}: whether the server
 *   started in GM-only mode (only game masters may log in). A consumer SHOULD
 *   mute its "server is up" notification when this is {@code "true"}.</li>
 * </ul>
 */
public final class WellKnownServerStartMetadata {

    private WellKnownServerStartMetadata() {
    }

    public static final String GM_ONLY = "gm_only";
}
