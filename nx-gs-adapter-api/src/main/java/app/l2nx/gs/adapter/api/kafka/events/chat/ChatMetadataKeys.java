package app.l2nx.gs.adapter.api.kafka.events.chat;

/**
 * Keys the platform reads from {@link ChatMessageEvent#getMetadata()}; the map stays open, but these carry agreed meaning.
 */
public final class ChatMetadataKeys {

    /**
     * Speaker's clan id on the {@code CLAN} / {@code ALLIANCE} channels, as a decimal string.
     * Written by the host because only it knows the clan at the moment of speaking — the platform's
     * own replica lags, so a message from a character who just left the clan would be scoped wrong.
     */
    public static final String CLAN_ID = "clanId";

    /**
     * Where the message was typed, when it did not come from the game client — e.g. {@code TMA}.
     * Absent for anything a player said in-game, which is what lets analysis separate real chat
     * from platform-originated traffic.
     */
    public static final String SOURCE = "source";

    /** The build's numeric chat type, as a decimal string — diagnostics for unmapped channels. */
    public static final String RAW_TYPE = "rawType";

    private ChatMetadataKeys() {}
}
