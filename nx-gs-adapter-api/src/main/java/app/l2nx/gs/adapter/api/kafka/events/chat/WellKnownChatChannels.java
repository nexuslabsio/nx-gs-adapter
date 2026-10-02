package app.l2nx.gs.adapter.api.kafka.events.chat;

/**
 * Canonical {@link ChatMessageEvent#getChannel() channel} codes ({@code UPPER_SNAKE_CASE}). A channel a build exposes
 * but this catalog does not name is published as {@code UNKNOWN_<int>}: routable, but not canonically aggregable.
 *
 * <ul>
 *   <li>{@link #WHISPER} - private tell; {@code targetCharId} / {@code targetCharName} are set.</li>
 *   <li>{@link #MSN} - external IM relay.</li>
 *   <li>{@link #COMMAND_CHANNEL_COMMANDER} - command-channel leaders only.</li>
 * </ul>
 */
public final class WellKnownChatChannels {

    private WellKnownChatChannels() {}

    public static final String GENERAL = "GENERAL";
    public static final String SHOUT = "SHOUT";
    public static final String WHISPER = "WHISPER";
    public static final String PARTY = "PARTY";
    public static final String CLAN = "CLAN";
    public static final String ALLIANCE = "ALLIANCE";
    public static final String TRADE = "TRADE";
    public static final String WORLD = "WORLD";
    public static final String HERO = "HERO";
    public static final String GM = "GM";
    public static final String PETITION = "PETITION";
    public static final String PETITION_GM = "PETITION_GM";
    public static final String ANNOUNCEMENT = "ANNOUNCEMENT";
    public static final String CRITICAL_ANNOUNCEMENT = "CRITICAL_ANNOUNCEMENT";
    public static final String SCREEN_ANNOUNCEMENT = "SCREEN_ANNOUNCEMENT";
    public static final String BATTLEFIELD = "BATTLEFIELD";
    public static final String BOAT = "BOAT";
    public static final String FRIEND = "FRIEND";
    public static final String MSN = "MSN";
    public static final String PARTY_ROOM = "PARTY_ROOM";
    public static final String COMMAND_CHANNEL = "COMMAND_CHANNEL";
    public static final String COMMAND_CHANNEL_COMMANDER = "COMMAND_CHANNEL_COMMANDER";
    public static final String NPC_GENERAL = "NPC_GENERAL";
    public static final String NPC_SHOUT = "NPC_SHOUT";
    public static final String NPC_WHISPER = "NPC_WHISPER";
}
