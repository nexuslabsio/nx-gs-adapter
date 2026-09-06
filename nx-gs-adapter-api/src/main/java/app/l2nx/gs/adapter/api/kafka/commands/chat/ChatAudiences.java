package app.l2nx.gs.adapter.api.kafka.commands.chat;

/**
 * Recipient-set codes used as the {@link SendChatMessageCommand#getAudience()
 * audience} value. Open-string vocabulary ({@code UPPER_SNAKE_CASE}), mirroring
 * the style of the channel catalog.
 *
 * <p>The audience is deliberately a separate axis from the channel: a whisper to
 * one player and a whisper fanned out to everyone online are the same frame with
 * different recipient lists, so folding the two into a single enum would force a
 * second command carrying a copy of every field.</p>
 *
 * <p>Adding a new constant is a non-breaking minor-version change. A host that
 * does not recognize a code replies {@code VALIDATION_FAILED} rather than
 * guessing a recipient set.</p>
 */
public final class ChatAudiences {

    /**
     * One character, named by {@link SendChatMessageCommand#getAudienceId()}.
     * The recipient must be in the world — an offline character has no session
     * to receive the packet.
     */
    public static final String CHARACTER = "CHARACTER";

    /**
     * Every online member of the clan named by
     * {@link SendChatMessageCommand#getAudienceId()}. The speaker himself need
     * NOT be online: the host resolves the clan from its own tables.
     */
    public static final String CLAN = "CLAN";

    /** Every player currently in the world. {@code audienceId} is unused. */
    public static final String ALL_ONLINE = "ALL_ONLINE";

    private ChatAudiences() {}
}
