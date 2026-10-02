package app.l2nx.gs.adapter.api.kafka.commands.chat;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import java.util.UUID;

/**
 * Puts one line of text into game chat: a player speaking from outside the client (even while offline) or the
 * platform under any display name. Counterpart of {@link app.l2nx.gs.adapter.api.kafka.events.chat.ChatMessageEvent}.
 *
 * <p>{@code NOT_FOUND}: unknown sender or audience. {@code FORBIDDEN}: chat/shadow ban, block list, channel floor.
 * {@code VALIDATION_FAILED}: missing field, channel outside the host whitelist, unknown audience, audienceId
 * missing or (for ALL_ONLINE) present. {@code INTERNAL_ERROR}: broadcast failed host-side.</p>
 *
 * <p>{@link #getSenderCharacterId()} is who speaks legally (host gates, packet object id, attribution);
 * {@link #getSenderDisplayName()} is only what the client renders. A persona announcement has a name and no character.</p>
 *
 * <p>Delivery is at-most-once (see {@link app.l2nx.gs.adapter.api.spi.capability.CommandHandler}); a re-issue after a reply timeout looks like a fresh
 * request, so {@code messageId} dedup works only if the host keeps a window of seen ids.</p>
 *
 * <p>Gson bypasses the constructor, so the handler must re-validate required fields.</p>
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
     * UUIDv7 minted by the platform; echoed as the echo event's {@code eventId} and used as dedup key.
     */
    public UUID getMessageId() {
        return messageId;
    }

    /** Host rejects codes outside its whitelist with {@code VALIDATION_FAILED} rather than rerouting. */
    public String getChannel() {
        return channel;
    }

    public String getAudience() {
        return audience;
    }

    /**
     * Character id for {@code CHARACTER}, clan id for {@code CLAN}; {@code null} iff {@code ALL_ONLINE}.
     */
    public Long getAudienceId() {
        return audienceId;
    }

    /** {@code null} means the platform speaks: no gates apply and the packet carries no object id. */
    public Long getSenderCharacterId() {
        return senderCharacterId;
    }

    /**
     * Composed in full by the platform and written verbatim by the host; never {@code null}, empty means nameless announcement.
     */
    public String getSenderDisplayName() {
        return senderDisplayName;
    }

    /**
     * Origin surface, e.g. {@code TMA} or {@code AUTO_ANNOUNCEMENT}; echoed into event metadata under
     * {@link app.l2nx.gs.adapter.api.kafka.events.chat.ChatMetadataKeys#SOURCE} so analysis can separate platform traffic from in-game typing.
     */
    public String getSource() {
        return source;
    }

    /**
     * Neutral micro-format: plain text, literal {@code \n} line breaks, bare {@code http(s)://} URLs; the host translates to wire tokens.
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
