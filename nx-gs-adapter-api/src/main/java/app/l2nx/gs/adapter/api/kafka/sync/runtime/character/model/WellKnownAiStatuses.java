package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.CharacterRuntimeDto;

/**
 * Canonical lower_snake_case values for {@link CharacterRuntimeDto#getAiStatus()}, the engine-native control
 * intention. Open vocabulary: hosts may emit other values, consumers treat unknown ones as opaque.
 *
 * <p>Orthogonal to {@link WellKnownActivities}: a fishing character is typically {@link #IDLE} here.</p>
 */
public final class WellKnownAiStatuses {

    private WellKnownAiStatuses() {}

    public static final String IDLE = "idle";

    /**
     * Passive ready state, reacting to events.
     */
    public static final String ACTIVE = "active";

    public static final String REST = "rest";

    public static final String ATTACK = "attack";

    public static final String CAST = "cast";

    public static final String MOVING = "moving";

    public static final String FOLLOW = "follow";

    public static final String PICK_UP = "pick_up";

    public static final String INTERACT = "interact";
}
