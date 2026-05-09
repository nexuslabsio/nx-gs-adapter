package app.l2nx.gs.adapter.api.kafka.events.premium;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO published to the {@code premium} family topic
 * ({@code <tenant>.gs.events.premiumpurchase}) when a player buys items / services
 * inside the game world. Combined item+service baskets are first-class —
 * a single purchase event MAY carry any mix of items and services.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery).</p>
 *
 * <p>Soft invariant: {@code items.size() + services.size() &gt;= 1}. Producers
 * MUST NOT emit an empty event; the wire schema permits it, the platform
 * consumer logs and dedupes rather than rejecting.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson can deserialize without {@code @JsonProperty}.</p>
 */
public final class PremiumPurchaseEvent extends PremiumEvent {

    private final UUID eventId;
    private final long characterId;
    private final @Nullable String characterName;
    private final @Nullable String accountName;
    private final List<PurchaseItem> items;
    private final List<PurchaseService> services;

    public PremiumPurchaseEvent(UUID eventId,
                                long characterId,
                                @Nullable String characterName,
                                @Nullable String accountName,
                                @Nullable List<PurchaseItem> items,
                                @Nullable List<PurchaseService> services) {
        this.eventId = eventId;
        this.characterId = characterId;
        this.characterName = characterName;
        this.accountName = accountName;
        this.items = freezeList(items);
        this.services = freezeList(services);
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the
     * occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Source-side character primary key ({@code charId} / {@code objectId}).
     */
    public long getCharacterId() {
        return characterId;
    }

    /**
     * Character display name. Optional — host hooks may publish without it
     * when name lookup at the publish call site is awkward; the platform
     * resolves the name via its joined {@code db-sync.character} stream.
     */
    public @Nullable String getCharacterName() {
        return characterName;
    }

    /**
     * Owning account login. Optional.
     */
    public @Nullable String getAccountName() {
        return accountName;
    }

    /**
     * Item-grant lines. Always non-null on read; {@code null} passed to the
     * constructor is normalized to an empty list.
     */
    public List<PurchaseItem> getItems() {
        return items == null ? Collections.emptyList() : items;
    }

    /**
     * Service-applied lines. Always non-null on read; {@code null} passed to
     * the constructor is normalized to an empty list.
     */
    public List<PurchaseService> getServices() {
        return services == null ? Collections.emptyList() : services;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .characterId(characterId)
                .characterName(characterName)
                .accountName(accountName)
                .items(items)
                .services(services);
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
                && Objects.equals(services, that.services);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, characterId, characterName, accountName, items, services);
    }

    @Override
    public String toString() {
        return "PremiumPurchaseEvent[eventId=" + eventId
                + ", characterId=" + characterId
                + ", characterName=" + characterName
                + ", accountName=" + accountName
                + ", items=" + items
                + ", services=" + services + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long characterId;
        private @Nullable String characterName;
        private @Nullable String accountName;
        private @Nullable List<PurchaseItem> items;
        private @Nullable List<PurchaseService> services;

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

        public PremiumPurchaseEvent build() {
            return new PremiumPurchaseEvent(eventId, characterId, characterName,
                    accountName, items, services);
        }
    }
}
