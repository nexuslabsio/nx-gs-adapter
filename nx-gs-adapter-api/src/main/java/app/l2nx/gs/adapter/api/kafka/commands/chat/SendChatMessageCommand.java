package app.l2nx.gs.adapter.api.kafka.commands.chat;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * A shadow-banned or filtered speaker is not refused: reply is {@code OK}, nothing is delivered, the echo is flagged
 * {@link app.l2nx.gs.adapter.api.kafka.events.chat.ChatMetadataKeys#SHADOWED}. Delivery is at-most-once, so a
 * re-issue after a reply timeout looks fresh and {@code messageId} dedup needs a host-side window of seen ids.
 * Gson bypasses the constructor, so the handler must re-validate required fields.
 */
public final class SendChatMessageCommand implements NxCommand<SendChatMessageResult> {

    private final UUID messageId;
    private final String channel;
    private final String audience;
    private final @Nullable Long audienceId;
    private final @Nullable Long senderCharacterId;
    private final @Nullable String senderDisplayName;
    private final String source;
    private final String text;
    private final @Nullable String targetCharacterName;

    public SendChatMessageCommand(
            UUID messageId,
            String channel,
            String audience,
            @Nullable Long audienceId,
            @Nullable Long senderCharacterId,
            @Nullable String senderDisplayName,
            String source,
            String text,
            @Nullable String targetCharacterName) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.channel = requireText(channel, "channel");
        this.audience = requireText(audience, "audience");
        validateAudience(audience, audienceId, senderCharacterId, targetCharacterName);
        if (senderCharacterId != null && senderCharacterId <= 0) {
            throw new IllegalArgumentException("senderCharacterId must be positive (got " + senderCharacterId + ")");
        }
        this.audienceId = audienceId;
        this.senderCharacterId = senderCharacterId;
        if (senderCharacterId == null) {
            Objects.requireNonNull(senderDisplayName, "senderDisplayName");
        }
        this.senderDisplayName = senderDisplayName;
        this.source = requireText(source, "source");
        this.text = requireText(text, "text");
        this.targetCharacterName = targetCharacterName;
    }

    private static void validateAudience(
            String audience, Long audienceId, Long senderCharacterId, String targetCharacterName) {
        if (ChatAudiences.CHARACTER.equals(audience)) {
            boolean hasName =
                    targetCharacterName != null && !targetCharacterName.trim().isEmpty();
            if (targetCharacterName != null && !hasName) {
                throw new IllegalArgumentException("targetCharacterName must not be blank");
            }
            if ((audienceId != null) == hasName) {
                throw new IllegalArgumentException(
                        "exactly one of audienceId / targetCharacterName is required for audience=CHARACTER");
            }
            return;
        }
        if (targetCharacterName != null) {
            throw new IllegalArgumentException("targetCharacterName is only allowed for audience=CHARACTER");
        }
        if (ChatAudiences.ALL_ONLINE.equals(audience)) {
            if (audienceId != null) {
                throw new IllegalArgumentException("audienceId must be null for audience=ALL_ONLINE");
            }
        } else if (ChatAudiences.PARTY.equals(audience)) {
            if (audienceId != null) {
                throw new IllegalArgumentException("audienceId must be null for audience=PARTY");
            }
            if (senderCharacterId == null) {
                throw new IllegalArgumentException("senderCharacterId is required for audience=PARTY");
            }
        } else if (audienceId == null) {
            throw new IllegalArgumentException("audienceId is required for audience=" + audience);
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getChannel() {
        return channel;
    }

    public String getAudience() {
        return audience;
    }

    public @Nullable Long getAudienceId() {
        return audienceId;
    }

    /** {@code null} means the platform speaks: no gates apply and the packet carries no object id. */
    public @Nullable Long getSenderCharacterId() {
        return senderCharacterId;
    }

    public @Nullable String getSenderDisplayName() {
        return senderDisplayName;
    }

    public String getSource() {
        return source;
    }

    /** Plain text, literal {@code
     * } line breaks, bare {@code http(s)://} URLs; the host translates to wire tokens. */
    public String getText() {
        return text;
    }

    public @Nullable String getTargetCharacterName() {
        return targetCharacterName;
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
                .text(text)
                .targetCharacterName(targetCharacterName);
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
                && Objects.equals(text, that.text)
                && Objects.equals(targetCharacterName, that.targetCharacterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageId,
                channel,
                audience,
                audienceId,
                senderCharacterId,
                senderDisplayName,
                source,
                text,
                targetCharacterName);
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
                + ", text=" + text
                + ", targetCharacterName=" + targetCharacterName + "]";
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
        private @Nullable String targetCharacterName;

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

        public Builder audienceId(@Nullable Long audienceId) {
            this.audienceId = audienceId;
            return this;
        }

        public Builder senderCharacterId(@Nullable Long senderCharacterId) {
            this.senderCharacterId = senderCharacterId;
            return this;
        }

        public Builder senderDisplayName(@Nullable String senderDisplayName) {
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

        public Builder targetCharacterName(@Nullable String targetCharacterName) {
            this.targetCharacterName = targetCharacterName;
            return this;
        }

        public SendChatMessageCommand build() {
            return new SendChatMessageCommand(
                    messageId,
                    channel,
                    audience,
                    audienceId,
                    senderCharacterId,
                    senderDisplayName,
                    source,
                    text,
                    targetCharacterName);
        }
    }
}
