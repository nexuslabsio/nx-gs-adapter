package app.l2nx.gs.adapter.api.kafka.events.character;

import app.l2nx.gs.adapter.api.kafka.events.character.model.WellKnownDeathMetadata;
import app.l2nx.gs.adapter.api.kafka.events.character.model.WellKnownFarmModes;
import app.l2nx.gs.adapter.api.kafka.events.character.model.WellKnownKillerTypes;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Death of a character the host chooses to report; bohpts emits it only for unattended deaths (autofarm or auto-macro,
 * mode in the {@code farm_mode} metadata key). Shares the {@code character} topic with {@link CharacterPresenceEvent}
 * (dispatched by {@code Nx-Message-Type}) and the {@code charId} partition key.
 * {@code eventId} is a UUIDv7 idempotency key (at-least-once). {@code metadata} is an open map; canonical keys in
 * {@link WellKnownDeathMetadata}: {@code killer_type} ({@link WellKnownKillerTypes}), {@code killer_id} (character
 * object-id for {@code player}, NPC template-id for {@code monster}/{@code boss}), {@code farm_mode}
 * ({@link WellKnownFarmModes}). No killer name on the wire; the platform resolves it from the ids.
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
