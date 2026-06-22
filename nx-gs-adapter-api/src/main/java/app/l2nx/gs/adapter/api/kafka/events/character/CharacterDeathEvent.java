package app.l2nx.gs.adapter.api.kafka.events.character;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Discrete death fact — one event per character death that the host chooses to
 * report. bohpts emits this <b>only</b> when the dying character was unattended
 * at the moment of death — on autofarm or on an auto-macro (the legacy-bot
 * "your unattended character died" signal); the {@code farm_mode} metadata key
 * carries which mode. Attended deaths produce no event.
 *
 * <p>Second message type on the {@code character} family
 * ({@code <tenant>.gs.events.character}), alongside {@link CharacterPresenceEvent};
 * the two share a topic and are dispatched by the {@code Nx-Message-Type} header.
 * Partitioned by {@link #getCharId() charId} — same key as presence — so a
 * character's presence and death history land on one partition in occurrence
 * order.</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency key for
 *   at-least-once delivery; platform extracts {@code occurredAt} from the
 *   time-ordered prefix.</li>
 *   <li>{@link #getCharId() charId} — REQUIRED. The character that died; also
 *   the Kafka partition key.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about the death. {@code null} when absent.
 *   Canonical keys in {@link WellKnownDeathMetadata}: {@code killer_type}
 *   (a {@link WellKnownKillerTypes} value) and {@code killer_id} (the killer's
 *   character object-id for a {@code player} killer, or the killer's NPC
 *   template-id for a {@code monster} / {@code boss} killer), plus {@code farm_mode}
 *   (a {@link WellKnownFarmModes} value classifying the unattended mode). The
 *   platform resolves the killer's display name from those ids against its own
 *   catalogs; no killer name is carried on the wire. Hosts MAY publish arbitrary
 *   non-canonical keys; consumers ignore keys they do not understand.</li>
 * </ul>
 */
public final class CharacterDeathEvent {

    private final UUID eventId;
    private final long charId;
    private final @Nullable Map<String, String> metadata;

    public CharacterDeathEvent(UUID eventId, long charId, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "CharacterDeathEvent.eventId is required");
        this.charId = charId;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharId() {
        return charId;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).charId(charId).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterDeathEvent)) return false;
        CharacterDeathEvent that = (CharacterDeathEvent) o;
        return charId == that.charId && eventId.equals(that.eventId) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, charId, metadata);
    }

    @Override
    public String toString() {
        return "CharacterDeathEvent[eventId=" + eventId + ", charId=" + charId + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long charId;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public CharacterDeathEvent build() {
            return new CharacterDeathEvent(eventId, charId, metadata);
        }
    }
}
