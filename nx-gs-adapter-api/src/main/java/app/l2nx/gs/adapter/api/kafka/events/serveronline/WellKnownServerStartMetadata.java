package app.l2nx.gs.adapter.api.kafka.events.serveronline;

/**
 * Canonical key constants for the {@code metadata} map of the server-lifecycle
 * events {@link ServerStartedEvent} and {@link ServerStoppingEvent}. The map is
 * open — hosts MAY publish arbitrary additional keys; consumers treat unknown
 * keys as opaque. Adding a constant here is a non-breaking minor-version change.
 *
 * <ul>
 *   <li>{@link #GM_ONLY} — {@code "true"} / {@code "false"}: whether the server
 *   is in GM-only mode (only game masters may log in). The host always reports
 *   it; a consumer SHOULD mute its "server is up" / "server is stopping"
 *   notification when this is {@code "true"} (GM-only runs are operator tests).</li>
 *   <li>{@link #AUTO_RESTART} — {@code "true"} / {@code "false"}: whether this
 *   lifecycle fact was emitted as part of an automatic scheduled restart (e.g. the
 *   host's daily maintenance restart). The host always reports it; a consumer SHOULD
 *   mute its "server is up" / "server is stopping" notification when this is
 *   {@code "true"} (a scheduled restart is routine, not a player-facing announcement)
 *   while still persisting the lifecycle fact.</li>
 * </ul>
 */
public final class WellKnownServerStartMetadata {

    private WellKnownServerStartMetadata() {}

    public static final String GM_ONLY = "gm_only";

    public static final String AUTO_RESTART = "auto_restart";
}
