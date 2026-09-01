package app.l2nx.gs.adapter.api.kafka.events.characterlog;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Discrete fact about a character that is not recoverable from synced state — the <b>moment</b> a
 * threshold was crossed, not the resulting state the CDC rail already replicates. Sole message type
 * of the {@code characterlog} family ({@code <tenant>.gs.events.character.log}).
 *
 * <p>Discriminated by an open {@link #getType() type} with an open
 * {@link #getMetadata() metadata} map rather than a class per fact, so a host can ship a new fact
 * kind without a synchronised release of this API and every consumer.</p>
 *
 * <p>{@code eventId} is a UUIDv7 — the idempotency key, and the source of {@code occurredAt}, which
 * is why no timestamp is carried. {@code charId} is also the partition key. Metadata values are
 * decimal strings, as everywhere else in this API.</p>
 */
public final class CharacterLogEvent {

    private final UUID eventId;
    private final long charId;
    private final String type;
    private final @Nullable Map<String, String> metadata;

    public CharacterLogEvent(UUID eventId, long charId, String type, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "CharacterLogEvent.eventId is required");
        this.charId = charId;
        this.type = Objects.requireNonNull(type, "CharacterLogEvent.type is required");
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharId() {
        return charId;
    }

    public String getType() {
        return type;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).charId(charId).type(type).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterLogEvent)) return false;
        CharacterLogEvent that = (CharacterLogEvent) o;
        return charId == that.charId
                && eventId.equals(that.eventId)
                && type.equals(that.type)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, charId, type, metadata);
    }

    @Override
    public String toString() {
        return "CharacterLogEvent[eventId=" + eventId + ", charId=" + charId + ", type=" + type + ", metadata="
                + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long charId;
        private @Nullable String type;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public CharacterLogEvent build() {
            return new CharacterLogEvent(eventId, charId, type, metadata);
        }
    }
}
