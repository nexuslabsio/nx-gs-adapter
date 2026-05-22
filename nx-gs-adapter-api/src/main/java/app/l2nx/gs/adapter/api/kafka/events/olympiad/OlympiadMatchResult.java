package app.l2nx.gs.adapter.api.kafka.events.olympiad;

/**
 * Self-perspective Olympiad outcome. {@link #WIN} pairs with {@link #LOSS}
 * on the opponent's per-participant event; {@link #DRAW} pairs with
 * {@link #DRAW}.
 */
public enum OlympiadMatchResult {
    WIN,
    LOSS,
    DRAW
}
