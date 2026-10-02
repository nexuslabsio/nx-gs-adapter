package app.l2nx.gs.adapter.api.kafka.events.olympiad.model;

/**
 * Self-perspective reason an Olympiad match ended, mirrored across the two per-participant events
 * (e.g. {@link #OPPONENT_DISCONNECTED} pairs with {@link #SELF_DISCONNECTED}).
 */
public enum OlympiadMatchReason {
    NORMAL,
    OPPONENT_DEFAULTED,
    SELF_DEFAULTED,
    BOTH_DEFAULTED,
    OPPONENT_DISCONNECTED,
    SELF_DISCONNECTED,
    BOTH_DISCONNECTED,
    /**
     * Both offline at match-end without explicit crash flag; zero points delta.
     */
    BOTH_OFFLINE,
    TIMEOUT
}
