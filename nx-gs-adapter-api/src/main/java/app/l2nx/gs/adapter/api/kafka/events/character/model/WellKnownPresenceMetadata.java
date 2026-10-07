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
     * AFK / anti-bot kick, DDoS). A voluntary logout and a kick carry no {@code logout_reason}.
     */
    public static final String LOGOUT_REASON_DISCONNECT = "disconnect";

    /**
     * Set on the logout that closes the session of a character staying in the world as an offline trader. No session
     * is opened for the trader afterwards, including when the host restores it after a restart.
     */
    public static final String LOGOUT_REASON_OFFLINE_TRADE = "offline_trade";

    /**
     * Set on the logout sent for every player still in the world at server shutdown, so the session ends at the stop
     * rather than at a time the platform infers later.
     */
    public static final String LOGOUT_REASON_SERVER_STOP = "server_stop";
}
