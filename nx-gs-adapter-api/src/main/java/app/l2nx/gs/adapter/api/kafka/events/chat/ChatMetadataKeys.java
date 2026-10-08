package app.l2nx.gs.adapter.api.kafka.events.chat;

public final class ChatMetadataKeys {

    /** Decimal string, host-written: the platform replica lags and would scope a speaker who just left the clan wrong. */
    public static final String CLAN_ID = "clanId";

    public static final String ALLIANCE_ID = "allianceId";

    public static final String SOURCE = "source";

    /** {@code "true"} when delivered only to the speaker: store it (abuse signal) but show it only to the speaker. */
    public static final String SHADOWED = "shadowed";

    public static final String RAW_TYPE = "rawType";

    /** {@code "true"} when every online player received it, so no recipient list is sent; show it like {@code WORLD}. */
    public static final String SERVER_WIDE = "serverWide";

    private ChatMetadataKeys() {}
}
