package app.l2nx.gs.adapter.api.kafka.commands.chat;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import java.util.UUID;

/**
 * Inbound command putting one line of text into game chat — the outbound
 * counterpart of {@link app.l2nx.gs.adapter.api.kafka.events.chat.ChatMessageEvent}.
 * Covers a player speaking from outside the game client (mini app writing to
 * clan chat, including while his character is offline) and the platform itself
 * speaking under an arbitrary display name.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link SendChatMessageResult}{@code >}.
 * Common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — {@link #getSenderCharacterId() senderCharacterId}
 *     or {@link #getAudienceId() audienceId} resolves to nothing on this
 *     server.</li>
 *     <li>{@code FORBIDDEN} — host policy refuses: chat ban, shadow ban, block
 *     list, or a channel-specific floor such as the academy level gate.</li>
 *     <li>{@code VALIDATION_FAILED} — missing field, a {@link #getChannel()
 *     channel} outside the host's accepted whitelist, an unknown
 *     {@link #getAudience() audience}, or an {@code audienceId} absent where the
 *     audience requires one.</li>
 *     <li>{@code INTERNAL_ERROR} — the broadcast mechanism failed host-side.</li>
 * </ul>
 *
 * <p><b>Sender is two independent things.</b> {@link #getSenderCharacterId()}
 * is who speaks legally — it drives the host's gates, the chat packet's object
 * id and the platform's attribution — while {@link #getSenderDisplayName()} is
 * only what the client renders. A mini-app message carries both; a persona
 * announcement carries a display name and no character at all.</p>
 *
 * <p><b>The display name arrives composed in full.</b> The host writes it
 * verbatim, so the suffix convention ({@code "Vasya (TMA)"}) changes without an
 * adapter or game-core release. An empty string reproduces the nameless
 * announcement line.</p>
 *
 * <p><b>Idempotency.</b> {@link #getMessageId() messageId} is minted by the
 * platform and echoed as the {@code eventId} of the resulting
 * {@code ChatMessageEvent}. Delivery is at-most-once (see
 * {@link app.l2nx.gs.adapter.api.spi.CommandHandler}); what repeats is a caller
 * re-issuing after a reply timeout, which the handler cannot distinguish from a
 * fresh request. A host keeping a bounded window of seen ids converges on one
 * message; without that window the field is carried but buys nothing.</p>
 *
 * <p><b>Required fields.</b> {@link #getMessageId() messageId},
 * {@link #getChannel() channel}, {@link #getAudience() audience},
 * {@link #getSenderDisplayName() senderDisplayName}, {@link #getSource() source} and
 * {@link #getText() text} are REQUIRED; {@code audienceId} is required for every audience except
 * {@link ChatAudiences#ALL_ONLINE}, which forbids it. The constructor enforces
 * this via {@link IllegalArgumentException} for programmatic construction.
 * Wire-path Gson bypasses the constructor — handler-side null-checking is the
 * wire-validation gate.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class SendChatMessageCommand implements NxCommand<SendChatMessageResult> {

    private final UUID messageId;
    private final String channel;
    private final String audience;
    private final Long audienceId;
    private final Long senderCharacterId;
    private final String senderDisplayName;
    private final String source;
    private final String text;

    public SendChatMessageCommand(
            UUID messageId,
            String channel,
            String audience,
            Long audienceId,
            Long senderCharacterId,
            String senderDisplayName,
            String source,
            String text) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.channel = requireText(channel, "channel");
        this.audience = requireText(audience, "audience");
        if (ChatAudiences.ALL_ONLINE.equals(audience)) {
            if (audienceId != null) {
                throw new IllegalArgumentException("audienceId must be null for audience=ALL_ONLINE");
            }
        } else if (audienceId == null) {
            throw new IllegalArgumentException("audienceId is required for audience=" + audience);
        }
        if (senderCharacterId != null && senderCharacterId <= 0) {
            throw new IllegalArgumentException("senderCharacterId must be positive (got " + senderCharacterId + ")");
        }
        this.audienceId = audienceId;
        this.senderCharacterId = senderCharacterId;
        this.senderDisplayName = Objects.requireNonNull(senderDisplayName, "senderDisplayName");
        this.source = requireText(source, "source");
        this.text = requireText(text, "text");
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    /**
     * UUIDv7 minted by the platform, reused as the {@code eventId} of the echo
     * event and as the host's deduplication key.
     */
    public UUID getMessageId() {
        return messageId;
    }

    /**
     * {@link app.l2nx.gs.adapter.api.kafka.events.chat.WellKnownChatChannels}
     * code. Which codes a host accepts is its own whitelist — anything outside
     * it is answered {@code VALIDATION_FAILED} rather than silently rerouted.
     */
    public String getChannel() {
        return channel;
    }

    /** {@link ChatAudiences} code naming the recipient set. */
    public String getAudience() {
        return audience;
    }

    /**
     * Keyed subject of {@link #getAudience() audience} — character id for
     * {@code CHARACTER}, clan id for {@code CLAN}. {@code null} iff the audience
     * is {@code ALL_ONLINE}.
     */
    public Long getAudienceId() {
        return audienceId;
    }

    /**
     * Character the message is attributed to and whose chat restrictions gate
     * it. {@code null} means the platform itself speaks — no gates apply and
     * the chat packet carries no object id.
     */
    public Long getSenderCharacterId() {
        return senderCharacterId;
    }

    /**
     * Sender name as the client should render it, already composed by the
     * platform. Never {@code null}; empty means the nameless announcement form.
     */
    public String getSenderDisplayName() {
        return senderDisplayName;
    }

    /**
     * Where the message originates, e.g. {@code TMA} or {@code AUTO_ANNOUNCEMENT}. Required: a
     * message arriving through this command always came from somewhere on the platform, and the host
     * cannot infer which surface. Echoed verbatim into the event metadata under
     * {@link app.l2nx.gs.adapter.api.kafka.events.chat.ChatMetadataKeys#SOURCE}, which is what lets
     * analysis separate platform traffic from what players typed in-game.
     */
    public String getSource() {
        return source;
    }

    /**
     * Body in the neutral chat micro-format: plain text, literal {@code \n}
     * hard line breaks, bare {@code http(s)://} URLs for auto-linking.
     * Translating those into build-specific wire tokens is a host concern.
     */
    public String getText() {
        return text;
    }

    public Builder toBuilder() {
        return new Builder()
                .messageId(messageId)
                .channel(channel)
                .audience(audience)
                .audienceId(audienceId)
                .senderCharacterId(senderCharacterId)
                .senderDisplayName(senderDisplayName)
                .source(source)
                .text(text);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SendChatMessageCommand)) return false;
        SendChatMessageCommand that = (SendChatMessageCommand) o;
        return Objects.equals(messageId, that.messageId)
                && Objects.equals(channel, that.channel)
                && Objects.equals(audience, that.audience)
                && Objects.equals(audienceId, that.audienceId)
                && Objects.equals(senderCharacterId, that.senderCharacterId)
                && Objects.equals(senderDisplayName, that.senderDisplayName)
                && Objects.equals(source, that.source)
                && Objects.equals(text, that.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageId, channel, audience, audienceId, senderCharacterId, senderDisplayName, source, text);
    }

    @Override
    public String toString() {
        return "SendChatMessageCommand[messageId=" + messageId
                + ", channel=" + channel
                + ", audience=" + audience
                + ", audienceId=" + audienceId
                + ", senderCharacterId=" + senderCharacterId
                + ", senderDisplayName=" + senderDisplayName
                + ", source=" + source
                + ", text=" + text + "]";
    }

    public static final class Builder {
        private UUID messageId;
        private String channel;
        private String audience;
        private Long audienceId;
        private Long senderCharacterId;
        private String senderDisplayName;
        private String source;
        private String text;

        public Builder messageId(UUID messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder channel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder audience(String audience) {
            this.audience = audience;
            return this;
        }

        public Builder audienceId(Long audienceId) {
            this.audienceId = audienceId;
            return this;
        }

        public Builder senderCharacterId(Long senderCharacterId) {
            this.senderCharacterId = senderCharacterId;
            return this;
        }

        public Builder senderDisplayName(String senderDisplayName) {
            this.senderDisplayName = senderDisplayName;
            return this;
        }

        public Builder source(String source) {
            this.source = source;
            return this;
        }

        public Builder text(String text) {
            this.text = text;
            return this;
        }

        public SendChatMessageCommand build() {
            return new SendChatMessageCommand(
                    messageId, channel, audience, audienceId, senderCharacterId, senderDisplayName, source, text);
        }
    }
}
