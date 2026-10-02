package app.l2nx.gs.adapter.api.kafka.events.chat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Raw chat fact published to {@code <tenant>.gs.events.chat} for every player-typed message; pattern matching
 * lives in the platform.
 *
 * <p>{@code eventId} is a UUIDv7: its upper 48 bits carry the occurrence time (no {@code occurredAt} field), and consumers dedupe on it (at-least-once delivery).</p>
 *
 * <p>{@code charId} is the sender id and partition key (8-byte big-endian). {@code 0} means no legal sender (the
 * platform itself spoke); consumers store it as an absent character, not object id zero.</p>
 *
 * <p>Target fields are set only on {@link WellKnownChatChannels#WHISPER}. {@code targetCharId} is {@code null}
 * when the recipient is offline or unresolved, while {@code targetCharName} may still hold the typed name.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} preserves constructor parameter names so Jackson / Gson bind without
 * {@code @JsonProperty}.</p>
 */
public final class ChatMessageEvent {

    private final UUID eventId;
    private final long charId;
    private final @Nullable String charName;
    private final String channel;
    private final String text;
    private final @Nullable Long targetCharId;
    private final @Nullable String targetCharName;
    private final @Nullable Map<String, String> metadata;

    public ChatMessageEvent(
            UUID eventId,
            long charId,
            @Nullable String charName,
            String channel,
            String text,
            @Nullable Long targetCharId,
            @Nullable String targetCharName,
            @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "eventId");
        this.charId = charId;
        this.charName = charName;
        this.channel = Objects.requireNonNull(channel, "channel");
        this.text = Objects.requireNonNull(text, "text");
        this.targetCharId = targetCharId;
        this.targetCharName = targetCharName;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharId() {
        return charId;
    }

    public @Nullable String getCharName() {
        return charName;
    }

    /**
     * {@link WellKnownChatChannels} code, or {@code UNKNOWN_<int>} for a build-specific channel the catalog does not name.
     */
    public String getChannel() {
        return channel;
    }

    public String getText() {
        return text;
    }

    /**
     * Whisper recipient id; {@code null} on other channels and when the recipient is offline / unresolved.
     */
    public @Nullable Long getTargetCharId() {
        return targetCharId;
    }

    public @Nullable String getTargetCharName() {
        return targetCharName;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .charId(charId)
                .charName(charName)
                .channel(channel)
                .text(text)
                .targetCharId(targetCharId)
                .targetCharName(targetCharName)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChatMessageEvent)) return false;
        ChatMessageEvent that = (ChatMessageEvent) o;
        return charId == that.charId
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(charName, that.charName)
                && Objects.equals(channel, that.channel)
                && Objects.equals(text, that.text)
                && Objects.equals(targetCharId, that.targetCharId)
                && Objects.equals(targetCharName, that.targetCharName)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, charId, charName, channel, text, targetCharId, targetCharName, metadata);
    }

    @Override
    public String toString() {
        return "ChatMessageEvent[eventId=" + eventId
                + ", charId=" + charId
                + ", charName=" + charName
                + ", channel=" + channel
                + ", text=" + text
                + ", targetCharId=" + targetCharId
                + ", targetCharName=" + targetCharName
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long charId;
        private @Nullable String charName;
        private String channel;
        private String text;
        private @Nullable Long targetCharId;
        private @Nullable String targetCharName;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder charName(@Nullable String charName) {
            this.charName = charName;
            return this;
        }

        public Builder channel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder text(String text) {
            this.text = text;
            return this;
        }

        public Builder targetCharId(@Nullable Long targetCharId) {
            this.targetCharId = targetCharId;
            return this;
        }

        public Builder targetCharName(@Nullable String targetCharName) {
            this.targetCharName = targetCharName;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public ChatMessageEvent build() {
            return new ChatMessageEvent(
                    eventId, charId, charName, channel, text, targetCharId, targetCharName, metadata);
        }
    }
}
