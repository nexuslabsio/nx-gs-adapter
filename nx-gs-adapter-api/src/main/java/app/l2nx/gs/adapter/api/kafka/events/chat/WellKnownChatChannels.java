package app.l2nx.gs.adapter.api.kafka.events.chat;

/**
 * Canonical chat-channel codes used as the {@link ChatMessageEvent#getChannel()
 * channel} value of a {@link ChatMessageEvent}. Open-string vocabulary
 * ({@code UPPER_SNAKE_CASE}): a host maps its build-specific numeric chat type
 * to one of these codes. A channel a given build exposes but this catalog does
 * not yet name is published as the raw string {@code UNKNOWN_<int>} — the
 * platform still sees it and routes it, but cannot aggregate it canonically.
 *
 * <p>Adding a new constant is a non-breaking minor-version change in
 * {@code nx-gs-adapter-api}.</p>
 *
 * <ul>
 *   <li>{@link #GENERAL} — local/all-range say.</li>
 *   <li>{@link #SHOUT} — region-wide shout.</li>
 *   <li>{@link #WHISPER} — private tell to one player
 *   ({@link ChatMessageEvent#getTargetCharId() targetCharId} /
 *   {@link ChatMessageEvent#getTargetCharName() targetCharName} set).</li>
 *   <li>{@link #PARTY} — party channel.</li>
 *   <li>{@link #CLAN} — clan channel.</li>
 *   <li>{@link #ALLIANCE} — alliance channel.</li>
 *   <li>{@link #TRADE} — trade channel.</li>
 *   <li>{@link #WORLD} — global world chat.</li>
 *   <li>{@link #HERO} — hero-voice broadcast.</li>
 *   <li>{@link #GM} — GM channel.</li>
 *   <li>{@link #PETITION} — player side of a support petition.</li>
 *   <li>{@link #PETITION_GM} — GM side of a support petition.</li>
 *   <li>{@link #ANNOUNCEMENT} — server announcement.</li>
 *   <li>{@link #CRITICAL_ANNOUNCEMENT} — critical (highlighted) announcement.</li>
 *   <li>{@link #SCREEN_ANNOUNCEMENT} — on-screen announcement.</li>
 *   <li>{@link #BATTLEFIELD} — battlefield / instanced-event channel.</li>
 *   <li>{@link #BOAT} — boat / vehicle channel.</li>
 *   <li>{@link #FRIEND} — friend-list private message.</li>
 *   <li>{@link #MSN} — external IM relay channel.</li>
 *   <li>{@link #PARTY_ROOM} — party matching room.</li>
 *   <li>{@link #COMMAND_CHANNEL} — command-channel broadcast.</li>
 *   <li>{@link #COMMAND_CHANNEL_COMMANDER} — command-channel leaders-only.</li>
 *   <li>{@link #NPC_GENERAL} — NPC local say.</li>
 *   <li>{@link #NPC_SHOUT} — NPC shout.</li>
 *   <li>{@link #NPC_WHISPER} — NPC private message.</li>
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
