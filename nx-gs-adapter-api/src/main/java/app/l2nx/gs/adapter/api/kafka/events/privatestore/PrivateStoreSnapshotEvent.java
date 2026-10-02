package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.kafka.events.privatestore.model.Offer;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.model.PrivateStoreSide;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Full order book (NOT a delta) for one {@code (itemTemplateId, side)} pair, published by a host daemon to the {@code privatestore} family topic when the pair changed since the previous tick.
 * <p>The host publishes only pairs whose hash of the canonical-sorted offers changed; consumers keep last-known state.
 * <p>Tombstone: when a tracked pair has no offers left, the host emits one event with {@link #getOffers() offers} {@code = []} and stops tracking it; repeated empty ticks are not republished.
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7 (upper 48 bits encode the timestamp); consumers dedupe on it (at-least-once) and order within {@code (itemTemplateId, side)} by it.
 * <p>Wire offer order is unspecified (consumers MUST re-sort); producers SHOULD sort by {@code (unitPrice ASC, traderId ASC, enchantLevel ASC)} before hashing to avoid insertion-order noise.
 */
public final class PrivateStoreSnapshotEvent {

    private final UUID eventId;
    private final long itemTemplateId;
    private final PrivateStoreSide side;
    private final List<Offer> offers;
    private final @Nullable Map<String, String> metadata;

    public PrivateStoreSnapshotEvent(
            UUID eventId,
            long itemTemplateId,
            PrivateStoreSide side,
            @Nullable List<Offer> offers,
            @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.itemTemplateId = itemTemplateId;
        this.side = side;
        this.offers = freezeList(offers);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    /** Kafka partition key (8-byte BE), keeping updates per template ordered / compactable. */
    public long getItemTemplateId() {
        return itemTemplateId;
    }

    /** {@link PrivateStoreSide#ASK ASK} aggregates offers from SELL stores, {@link PrivateStoreSide#BID BID} from BUY stores. */
    public PrivateStoreSide getSide() {
        return side;
    }

    /** Never null on read; {@code null} passed to the constructor becomes an empty list; an empty list is meaningful (tombstone). */
    public List<Offer> getOffers() {
        return offers;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .itemTemplateId(itemTemplateId)
                .side(side)
                .offers(offers)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<Offer> freezeList(@Nullable List<Offer> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<Offer>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrivateStoreSnapshotEvent)) return false;
        PrivateStoreSnapshotEvent that = (PrivateStoreSnapshotEvent) o;
        return itemTemplateId == that.itemTemplateId
                && Objects.equals(eventId, that.eventId)
                && side == that.side
                && Objects.equals(offers, that.offers)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, itemTemplateId, side, offers, metadata);
    }

    @Override
    public String toString() {
        return "PrivateStoreSnapshotEvent[eventId=" + eventId
                + ", itemTemplateId=" + itemTemplateId
                + ", side=" + side
                + ", offers=" + offers
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long itemTemplateId;
        private PrivateStoreSide side;
        private @Nullable List<Offer> offers;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder itemTemplateId(long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder side(PrivateStoreSide side) {
            this.side = side;
            return this;
        }

        public Builder offers(@Nullable List<Offer> offers) {
            this.offers = offers;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public PrivateStoreSnapshotEvent build() {
            return new PrivateStoreSnapshotEvent(eventId, itemTemplateId, side, offers, metadata);
        }
    }
}
