package app.l2nx.gs.adapter.api.kafka.events.character.model;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterDeathEvent;

/**
 * Values of the {@code killer_type} key ({@link WellKnownDeathMetadata#KILLER_TYPE}) of {@link CharacterDeathEvent}.
 * Open lower_snake_case string: a host MAY emit other values, consumers treat unknown ones as opaque.
 *
 * <ul>
 *   <li>{@link #MONSTER} - non-boss NPC (PvE).</li>
 *   <li>{@link #PLAYER} - another player (PvP).</li>
 *   <li>{@link #BOSS} - raid / grand boss.</li>
 *   <li>{@link #SELF} - self-inflicted (suicide skill, fall, environment).</li>
 * </ul>
 */
public final class WellKnownKillerTypes {

    private WellKnownKillerTypes() {}

    public static final String MONSTER = "monster";

    public static final String PLAYER = "player";

    public static final String BOSS = "boss";

    public static final String SELF = "self";
}
