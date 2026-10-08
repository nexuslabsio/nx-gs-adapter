package app.l2nx.gs.adapter.api.kafka.commands.chat;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.OwnerVerified;
import app.l2nx.gs.adapter.api.kafka.events.chat.WellKnownChatChannels;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * A shadow-banned or filtered speaker still gets {@code OK}; the echo is flagged
 * {@link app.l2nx.gs.adapter.api.kafka.events.chat.ChatMetadataKeys#SHADOWED}. At-most-once: a re-issue after a reply
 * timeout looks fresh, so {@code messageId} dedup needs a host-side window. Gson bypasses the constructor.
 */
public final class SendChatMessageCommand implements OwnerVerified, NxCommand<SendChatMessageResult> {

    private final UUID messageId;
    private final String channel;
    private final String audience;
    private final @Nullable Long audienceId;
    private final @Nullable Long senderCharacterId;
    private final @Nullable String senderDisplayName;
    private final String source;
    private final String text;
    private final @Nullable String targetCharacterName;
    private final boolean ownerVerified;

    public SendChatMessageCommand(
            UUID messageId,
            String channel,
            String audience,
            @Nullable Long audienceId,
            @Nullable Long senderCharacterId,
            @Nullable String senderDisplayName,
            String source,
            String text,
            @Nullable String targetCharacterName,
            boolean ownerVerified) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.channel = requireText(channel, "channel");
        this.audience = requireText(audience, "audience");
        validateAudience(audience, audienceId, senderCharacterId, targetCharacterName);
        if (senderCharacterId != null && senderCharacterId <= 0) {
            throw new IllegalArgumentException("senderCharacterId must be positive (got " + senderCharacterId + ")");
        }
        this.audienceId = audienceId;
        this.senderCharacterId = senderCharacterId;
        if (senderCharacterId == null && !WellKnownChatChannels.ANNOUNCEMENT.equals(channel)) {
            Objects.requireNonNull(senderDisplayName, "senderDisplayName");
        }
        this.senderDisplayName = senderDisplayName;
        this.source = requireText(source, "source");
        this.text = requireText(text, "text");
        this.targetCharacterName = targetCharacterName;
        this.ownerVerified = ownerVerified;
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

    /** Plain text with literal line breaks and bare URLs; the host translates them to wire tokens. */
    public String getText() {
        return text;
    }

    public @Nullable String getTargetCharacterName() {
        return targetCharacterName;
    }

    @Override
    public boolean isOwnerVerified() {
        return ownerVerified;
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
                .targetCharacterName(targetCharacterName)
                .ownerVerified(ownerVerified);
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
                && Objects.equals(targetCharacterName, that.targetCharacterName)
                && ownerVerified == that.ownerVerified;
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
                targetCharacterName,
                ownerVerified);
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
                + ", targetCharacterName=" + targetCharacterName
                + ", ownerVerified=" + ownerVerified + "]";
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
        private boolean ownerVerified;

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

        public Builder ownerVerified(boolean ownerVerified) {
            this.ownerVerified = ownerVerified;
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
                    targetCharacterName,
                    ownerVerified);
        }
    }
}
