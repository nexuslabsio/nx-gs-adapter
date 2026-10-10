package app.l2nx.gs.adapter.api.kafka.events.push;

import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * The last stack of a consumable left the character's inventory (used up, dropped, traded or stored); lags the game by
 * the host's save interval plus one sync tick.
 * Published by platform services to {@code <tenantSlug>.push.events}, keyed by {@code characterId}, with
 * {@code Nx-Message-Type} = simple class name and {@code Nx-Server-Id}. {@code eventId} is a UUIDv7 idempotency key.
 */
public final class ItemDepletedEvent {

    private final UUID eventId;
    private final long characterId;
    private final int itemTemplateId;

    public ItemDepletedEvent(UUID eventId, long characterId, int itemTemplateId) {
        this.eventId = Objects.requireNonNull(eventId, "ItemDepletedEvent.eventId is required");
        this.characterId = characterId;
        this.itemTemplateId = itemTemplateId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharacterId() {
        return characterId;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).characterId(characterId).itemTemplateId(itemTemplateId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemDepletedEvent)) return false;
        ItemDepletedEvent that = (ItemDepletedEvent) o;
        return characterId == that.characterId && itemTemplateId == that.itemTemplateId && eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, characterId, itemTemplateId);
    }

    @Override
    public String toString() {
        return "ItemDepletedEvent[eventId=" + eventId + ", characterId=" + characterId + ", itemTemplateId="
                + itemTemplateId + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long characterId;
        private int itemTemplateId;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder characterId(long characterId) {
            this.characterId = characterId;
            return this;
        }

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public ItemDepletedEvent build() {
            return new ItemDepletedEvent(eventId, characterId, itemTemplateId);
        }
    }
}
