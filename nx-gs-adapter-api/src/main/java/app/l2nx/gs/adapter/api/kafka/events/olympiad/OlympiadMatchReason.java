package app.l2nx.gs.adapter.api.kafka.events.olympiad;

/**
 * Self-perspective reason an Olympiad match ended. Combined with
 * {@link OlympiadMatchResult}, fully describes the outcome. Mirrors
 * across the two per-participant events of one match (e.g.
 * {@link #OPPONENT_DISCONNECTED} pairs with {@link #SELF_DISCONNECTED}).
 */
public enum OlympiadMatchReason {
    /**
     * Fight concluded by HP / death / damage tiebreak.
     */
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
    /**
     * Both alive at time-out bell.
     */
    TIMEOUT
}
