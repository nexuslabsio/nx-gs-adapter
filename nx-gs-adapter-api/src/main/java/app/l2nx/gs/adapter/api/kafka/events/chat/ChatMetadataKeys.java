package app.l2nx.gs.adapter.api.kafka.events.chat;

public final class ChatMetadataKeys {

    /**
     * Speaker's clan id, decimal string. Host-written: the platform replica lags, so a speaker who just left the clan
     * would be scoped wrong.
     */
    public static final String CLAN_ID = "clanId";

    public static final String ALLIANCE_ID = "allianceId";

    /** Set only for traffic not typed in-game, e.g. {@code MINIAPP}. */
    public static final String SOURCE = "source";

    /**
     * {@code "true"} when delivered only to the speaker (shadow ban, filter). Consumers must store it (abuse signal) but
     * show it only to the speaker.
     */
    public static final String SHADOWED = "shadowed";

    public static final String RAW_TYPE = "rawType";

    private ChatMetadataKeys() {}
}
