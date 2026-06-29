package app.l2nx.gs.adapter.api.kafka.commands.ban;

/**
 * Canonical values for the {@code banType} field on {@link BanCommand} /
 * {@link UnbanCommand} and {@code banType} on
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.ban.BanDbDto}. The
 * field is an <b>open string</b> so a host shipping a new ban kind is not
 * a breaking contract change. Mirrors the {@code WellKnown*} pattern on the
 * other DTOs; the platform stores unknown values verbatim. Values are
 * {@code UPPER_SNAKE_CASE}.
 *
 * <p>These are the platform-canonical names; a host maps its own engine
 * ban enum onto them (e.g. an L2J fork's {@code BAN} → {@link #GAME_LOGIN},
 * {@code CHAT_BAN} → {@link #CHAT}). The contract names <i>what is restricted</i>,
 * not how the host enforces it.</p>
 *
 * <ul>
 *   <li>{@link #GAME_LOGIN} — blocks entering the game (login rejected).</li>
 *   <li>{@link #CHAT} — visible chat mute: the player is told chat is forbidden
 *   on the affected channels.</li>
 *   <li>{@link #CHAT_SHADOW} — silent chat mute: the player's messages reach only
 *   themselves; other players never see them.</li>
 *   <li>{@link #PARTY} — blocks forming / joining a party.</li>
 *   <li>{@link #JAIL} — confines the character to the jail zone; duration is
 *   counted in online time.</li>
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
