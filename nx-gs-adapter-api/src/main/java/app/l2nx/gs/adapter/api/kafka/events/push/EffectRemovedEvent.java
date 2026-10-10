package app.l2nx.gs.adapter.api.kafka.events.push;

import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * A buff of the previous runtime snapshot is gone from the next while the character is alive (expired, dispelled or
 * cancelled, not told apart); a level change of the same skill is not a removal.
 * Published by platform services to {@code <tenantSlug>.push.events}, keyed by {@code characterId}, with
 * {@code Nx-Message-Type} = simple class name and {@code Nx-Server-Id}. {@code eventId} is a UUIDv7 idempotency key.
 */
public final class EffectRemovedEvent {

    private final UUID eventId;
    private final long characterId;
    private final int skillId;

    public EffectRemovedEvent(UUID eventId, long characterId, int skillId) {
        this.eventId = Objects.requireNonNull(eventId, "EffectRemovedEvent.eventId is required");
        this.characterId = characterId;
        this.skillId = skillId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharacterId() {
        return characterId;
    }

    public int getSkillId() {
        return skillId;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).characterId(characterId).skillId(skillId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EffectRemovedEvent)) return false;
        EffectRemovedEvent that = (EffectRemovedEvent) o;
        return characterId == that.characterId && skillId == that.skillId && eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, characterId, skillId);
    }

    @Override
    public String toString() {
        return "EffectRemovedEvent[eventId=" + eventId + ", characterId=" + characterId + ", skillId=" + skillId + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long characterId;
        private int skillId;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder characterId(long characterId) {
            this.characterId = characterId;
            return this;
        }

        public Builder skillId(int skillId) {
            this.skillId = skillId;
            return this;
        }

        public EffectRemovedEvent build() {
            return new EffectRemovedEvent(eventId, characterId, skillId);
        }
    }
}
