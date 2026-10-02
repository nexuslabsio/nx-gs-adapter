package app.l2nx.gs.adapter.api.kafka.events.serveronline.model;

import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerStartedEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerStoppingEvent;

/**
 * Canonical keys of the {@code metadata} map of {@link ServerStartedEvent} and {@link ServerStoppingEvent}.
 * Hosts MAY publish other keys; consumers treat unknown keys as opaque.
 *
 * <ul>
 *   <li>{@link #GM_ONLY} - {@code "true"} / {@code "false"}: only game masters may log in. Consumers SHOULD
 *   mute up / stopping notifications when {@code "true"} (operator tests).</li>
 *   <li>{@link #AUTO_RESTART} - {@code "true"} / {@code "false"}: fact belongs to an automatic scheduled
 *   restart. Consumers SHOULD mute notifications when {@code "true"} but still persist the fact.</li>
 * </ul>
 */
public final class WellKnownServerStartMetadata {

    private WellKnownServerStartMetadata() {}

    public static final String GM_ONLY = "gm_only";

    public static final String AUTO_RESTART = "auto_restart";
}
