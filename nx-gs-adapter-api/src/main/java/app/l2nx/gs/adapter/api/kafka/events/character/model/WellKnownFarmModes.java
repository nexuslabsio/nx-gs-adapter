package app.l2nx.gs.adapter.api.kafka.events.character.model;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterDeathEvent;

/**
 * Values of the {@code farm_mode} key ({@link WellKnownDeathMetadata#FARM_MODE}) of {@link CharacterDeathEvent}.
 * Open lower_snake_case string: a host MAY emit other modes, consumers treat unknown values as opaque.
 *
 * <ul>
 *   <li>{@link #AUTOFARM} - server-side auto-hunt bot.</li>
 *   <li>{@link #AUTO_MACRO} - server-managed official cycle-macro session.</li>
 * </ul>
 */
public final class WellKnownFarmModes {

    private WellKnownFarmModes() {}

    public static final String AUTOFARM = "autofarm";

    public static final String AUTO_MACRO = "auto_macro";
}
