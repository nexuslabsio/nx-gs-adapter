package app.l2nx.gs.adapter.api.kafka.commands.chat;

public final class ChatAudiences {

    /** The addressee may be offline: no packet is sent, but the message is gated and echoed. */
    public static final String CHARACTER = "CHARACTER";

    public static final String CLAN = "CLAN";

    public static final String ALLIANCE = "ALLIANCE";

    public static final String PARTY = "PARTY";

    public static final String ALL_ONLINE = "ALL_ONLINE";

    private ChatAudiences() {}
}
