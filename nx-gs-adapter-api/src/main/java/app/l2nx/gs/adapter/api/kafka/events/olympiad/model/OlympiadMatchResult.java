package app.l2nx.gs.adapter.api.kafka.events.olympiad.model;

/** Self-perspective outcome; {@link #WIN} pairs with {@link #LOSS} on the opponent's event, {@link #DRAW} with {@link #DRAW}. */
public enum OlympiadMatchResult {
    WIN,
    LOSS,
    DRAW
}
