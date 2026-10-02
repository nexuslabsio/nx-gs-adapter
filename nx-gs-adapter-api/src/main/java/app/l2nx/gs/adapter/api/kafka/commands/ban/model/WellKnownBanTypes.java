package app.l2nx.gs.adapter.api.kafka.commands.ban.model;

import app.l2nx.gs.adapter.api.kafka.commands.ban.BanCommand;
import app.l2nx.gs.adapter.api.kafka.commands.ban.UnbanCommand;

/**
 * Canonical {@code banType} values on {@link BanCommand} / {@link UnbanCommand} and
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.ban.BanDbDto}. Open string so a new ban kind is not a breaking
 * change; unknown values are stored verbatim. {@code UPPER_SNAKE_CASE}. Names what is restricted, not how the
 * host enforces it.
 *
 * <ul>
 *   <li>{@link #GAME_LOGIN} - login rejected.</li>
 *   <li>{@link #CHAT} - visible mute: the player is told chat is forbidden.</li>
 *   <li>{@link #CHAT_SHADOW} - silent mute: messages reach only the sender.</li>
 *   <li>{@link #PARTY} - blocks forming / joining a party.</li>
 *   <li>{@link #JAIL} - confines the character to the jail zone; duration counts online time.</li>
 * </ul>
 */
public final class WellKnownBanTypes {

    private WellKnownBanTypes() {}

    public static final String GAME_LOGIN = "GAME_LOGIN";
    public static final String CHAT = "CHAT";
    public static final String CHAT_SHADOW = "CHAT_SHADOW";
    public static final String PARTY = "PARTY";
    public static final String JAIL = "JAIL";
}
