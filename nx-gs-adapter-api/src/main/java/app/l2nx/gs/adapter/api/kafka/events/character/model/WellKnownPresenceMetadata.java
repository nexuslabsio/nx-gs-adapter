package app.l2nx.gs.adapter.api.kafka.events.character.model;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;

/**
 * Keys of the open {@code metadata} map of {@link CharacterPresenceEvent}; consumers ignore unknown keys.
 * Values are build-agnostic: host adapters map their own vocabulary onto them. {@code metadata} is
 * {@code null} on the common path (login, voluntary logout).
 */
public final class WellKnownPresenceMetadata {

    private WellKnownPresenceMetadata() {}

    public static final String LOGOUT_REASON = "logout_reason";

    /**
     * Set on a logout ({@code online=false}) caused by involuntary connection loss (network drop, client crash,
     * AFK / anti-bot kick, DDoS). Voluntary and server-initiated logouts carry no {@code logout_reason}.
     */
    public static final String LOGOUT_REASON_DISCONNECT = "disconnect";
}
