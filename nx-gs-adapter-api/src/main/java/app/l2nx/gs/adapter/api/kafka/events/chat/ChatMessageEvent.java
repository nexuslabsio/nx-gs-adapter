package app.l2nx.gs.adapter.api.kafka.events.chat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO published to the {@code chat} family topic
 * ({@code <tenant>.gs.events.chat}) for every player-typed chat message. The
 * adapter ships the raw fact — sender, channel, text — and all pattern matching
 * lives in the platform.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery).</p>
 *
 * <p>{@link #getCharId() charId} is the sender object id and the partition key
 * (8-byte big-endian) so one sender's messages keep occurrence order on a
 * single partition. It is a primitive and therefore cannot carry a wire null,
 * so {@code 0} means <em>no legal sender</em> — the platform itself spoke and
 * there is no character to attribute the line to. That follows the game's own
 * convention for a senderless chat packet; consumers store it as an absent
 * character rather than as object id zero.</p>
 *
 * <p>{@link #getChannel() channel} is a {@link WellKnownChatChannels} code (or
 * {@code UNKNOWN_<int>} for a build-specific channel this catalog does not yet
 * name). {@link #getText() text} is already sanitized host-side.</p>
 *
 * <p>{@link #getTargetCharId() targetCharId} /
 * {@link #getTargetCharName() targetCharName} are populated only for the
 * {@link WellKnownChatChannels#WHISPER WHISPER} channel; both are {@code null}
 * on every other channel. {@code targetCharId} is {@code null} when the
 * recipient is offline or cannot be resolved, while {@code targetCharName} may
 * still carry the typed recipient name.</p>
 *
 * <p>{@link #getMetadata() metadata} is an optional open string&rarr;string map
 * of build-agnostic attributes ({@code rawType} — the build's numeric chat
 * type, room id, etc.), {@code null} when absent. Hosts MAY add arbitrary keys
 * without an API release; consumers ignore unknown keys.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so parameter-name-binding deserializers (Jackson on the
 * platform, Gson on the adapter) bind without {@code @JsonProperty}.</p>
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

    /**
     * UUIDv7 — upper 48 bits encode occurredAt.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Sender object id. Partition key (8-byte BE).
     */
    public long getCharId() {
        return charId;
    }

    public @Nullable String getCharName() {
        return charName;
    }

    /**
     * {@link WellKnownChatChannels} code, or {@code UNKNOWN_<int>} for a
     * build-specific channel this catalog does not name.
     */
    public String getChannel() {
        return channel;
    }

    public String getText() {
        return text;
    }

    /**
     * Whisper recipient object id. Set only on
     * {@link WellKnownChatChannels#WHISPER WHISPER}; {@code null} on other
     * channels and when the recipient is offline / unresolved.
     */
    public @Nullable Long getTargetCharId() {
        return targetCharId;
    }

    /**
     * Whisper recipient name. Set only on
     * {@link WellKnownChatChannels#WHISPER WHISPER}.
     */
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
