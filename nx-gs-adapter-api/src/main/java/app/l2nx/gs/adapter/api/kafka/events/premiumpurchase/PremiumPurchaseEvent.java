package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase;

import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.model.PurchaseItem;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.model.PurchaseService;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Player buys items / services in the game world, on the {@code premiumpurchase} family topic; one event may mix items and services.
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7 (upper 48 bits encode the timestamp); consumers dedupe on it (at-least-once).
 * <p>Soft invariant: {@code items.size() + services.size() &gt;= 1}. Producers MUST NOT emit an empty event; the consumer logs and dedupes rather than rejecting.
 * <p>{@link #getMetadata() metadata} is an optional open string-to-string map; {@code null} when absent, consumers ignore unknown keys.
 */
public final class PremiumPurchaseEvent {

    private final UUID eventId;
    private final long characterId;
    private final @Nullable String characterName;
    private final @Nullable String accountName;
    private final List<PurchaseItem> items;
    private final List<PurchaseService> services;
    private final @Nullable Map<String, String> metadata;

    public PremiumPurchaseEvent(
            UUID eventId,
            long characterId,
            @Nullable String characterName,
            @Nullable String accountName,
            @Nullable List<PurchaseItem> items,
            @Nullable List<PurchaseService> services,
            @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.characterId = characterId;
        this.characterName = characterName;
        this.accountName = accountName;
        this.items = freezeList(items);
        this.services = freezeList(services);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharacterId() {
        return characterId;
    }

    /** Optional; the platform resolves it via the {@code db-sync.character} stream when absent. */
    public @Nullable String getCharacterName() {
        return characterName;
    }

    public @Nullable String getAccountName() {
        return accountName;
    }

    /** Never null on read; {@code null} passed to the constructor becomes an empty list. */
    public List<PurchaseItem> getItems() {
        return items == null ? Collections.emptyList() : items;
    }

    /** Never null on read; {@code null} passed to the constructor becomes an empty list. */
    public List<PurchaseService> getServices() {
        return services == null ? Collections.emptyList() : services;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .characterId(characterId)
                .characterName(characterName)
                .accountName(accountName)
                .items(items)
                .services(services)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static <T> List<T> freezeList(@Nullable List<T> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<T>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PremiumPurchaseEvent)) return false;
        PremiumPurchaseEvent that = (PremiumPurchaseEvent) o;
        return characterId == that.characterId
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(characterName, that.characterName)
                && Objects.equals(accountName, that.accountName)
                && Objects.equals(items, that.items)
                && Objects.equals(services, that.services)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, characterId, characterName, accountName, items, services, metadata);
    }

    @Override
    public String toString() {
        return "PremiumPurchaseEvent[eventId=" + eventId
                + ", characterId=" + characterId
                + ", characterName=" + characterName
                + ", accountName=" + accountName
                + ", items=" + items
                + ", services=" + services
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long characterId;
        private @Nullable String characterName;
        private @Nullable String accountName;
        private @Nullable List<PurchaseItem> items;
        private @Nullable List<PurchaseService> services;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder characterId(long characterId) {
            this.characterId = characterId;
            return this;
        }

        public Builder characterName(@Nullable String characterName) {
            this.characterName = characterName;
            return this;
        }

        public Builder accountName(@Nullable String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder items(@Nullable List<PurchaseItem> items) {
            this.items = items;
            return this;
        }

        public Builder services(@Nullable List<PurchaseService> services) {
            this.services = services;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public PremiumPurchaseEvent build() {
            return new PremiumPurchaseEvent(
                    eventId, characterId, characterName, accountName, items, services, metadata);
        }
    }
}
