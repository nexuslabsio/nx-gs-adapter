package app.l2nx.gs.adapter.api.kafka.commands.chat;

/**
 * Recipient-set codes for {@link SendChatMessageCommand#getAudience() audience}; a separate axis from channel.
 * Unknown codes get {@code VALIDATION_FAILED}; adding a constant is non-breaking.
 */
public final class ChatAudiences {

    /** Named by audienceId; must be online, an offline character has no session. */
    public static final String CHARACTER = "CHARACTER";

    /** Online clan members; the speaker need NOT be online. */
    public static final String CLAN = "CLAN";

    public static final String ALL_ONLINE = "ALL_ONLINE";

    private ChatAudiences() {}
}
