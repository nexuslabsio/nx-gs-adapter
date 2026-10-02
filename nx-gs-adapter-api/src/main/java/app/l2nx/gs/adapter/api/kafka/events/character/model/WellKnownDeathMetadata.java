package app.l2nx.gs.adapter.api.kafka.events.character.model;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterDeathEvent;

/**
 * Keys of the open {@code metadata} map of {@link CharacterDeathEvent}; consumers treat unknown keys as opaque.
 *
 * <ul>
 *   <li>{@link #KILLER_TYPE} - a {@link WellKnownKillerTypes} value; absent when the host does not classify.</li>
 *   <li>{@link #KILLER_ID} - decimal string: character object-id for {@code player}, NPC template-id for
 *   {@code monster}/{@code boss}; absent for {@code self} / unattributable deaths. No killer name is on the wire.</li>
 *   <li>{@link #FARM_MODE} - a {@link WellKnownFarmModes} value; set only on the unattended-death signal.</li>
 * </ul>
 */
public final class WellKnownDeathMetadata {

    private WellKnownDeathMetadata() {}

    public static final String KILLER_TYPE = "killer_type";

    public static final String KILLER_ID = "killer_id";

    public static final String FARM_MODE = "farm_mode";
}
