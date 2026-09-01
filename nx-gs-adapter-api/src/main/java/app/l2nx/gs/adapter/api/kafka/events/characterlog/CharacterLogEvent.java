package app.l2nx.gs.adapter.api.kafka.events.characterlog;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Discrete fact about a character that is not recoverable from synced state —
 * the <b>moment</b> a threshold was crossed, as opposed to the resulting state
 * the CDC rail already replicates. Sole message type of the {@code characterlog}
 * family ({@code <tenant>.gs.events.character.log}).
 *
 * <p>The payload is discriminated by {@link #getType() type} and carried in an
 * open {@link #getMetadata() metadata} map rather than by a class per fact. A
 * host can therefore ship a new fact kind without a synchronised release of this
 * API and every consumer: unknown types stay parseable, and a consumer learns to
 * read them later against data it has already been accumulating.</p>
 *
 * <p>Partitioned by {@link #getCharId() charId}, so one character's facts land on
 * one partition in occurrence order.</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency key for
 *   at-least-once delivery; the platform extracts {@code occurredAt} from the
 *   time-ordered prefix, so no timestamp is carried separately.</li>
 *   <li>{@link #getCharId() charId} — REQUIRED. The character the fact is about,
 *   and the Kafka partition key.</li>
 *   <li>{@link #getType() type} — REQUIRED. Open token classifying the fact.
 *   Canonical values in {@link WellKnownCharacterLogTypes}; hosts MAY publish
 *   tokens no consumer understands yet.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   per-type attributes; {@code null} when absent. Canonical keys in
 *   {@link WellKnownCharacterLogMetadata}. Numeric values travel as decimal
 *   strings, matching the rest of this API's metadata maps.</li>
 * </ul>
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
