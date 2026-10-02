package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.kafka.events.privatestore.model.PrivateStoreSide;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.model.TradeLine;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Private-store deal finalized on the game thread, on the {@code privatestore} family topic; one event per transaction, multi-line when several positions were bought atomically.
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7 (upper 48 bits encode the timestamp); consumers dedupe on it (at-least-once).
 * <p>{@link #getStoreType() storeType} says which party opened the store (maker/taker direction; see {@link PrivateStoreSide}).
 * <p>Soft invariant: {@code lines.size() &gt;= 1}. Producers MUST NOT emit an empty event; the consumer logs and dedupes rather than rejecting.
 */
public final class PrivateStorePurchaseEvent {

    private final UUID eventId;
    private final PrivateStoreSide storeType;
    private final long sellerId;
    private final @Nullable String sellerName;
    private final long buyerId;
    private final @Nullable String buyerName;
    private final List<TradeLine> lines;
    private final @Nullable Map<String, String> metadata;

    public PrivateStorePurchaseEvent(
            UUID eventId,
            PrivateStoreSide storeType,
            long sellerId,
            @Nullable String sellerName,
            long buyerId,
            @Nullable String buyerName,
            @Nullable List<TradeLine> lines,
            @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.storeType = storeType;
        this.sellerId = sellerId;
        this.sellerName = sellerName;
        this.buyerId = buyerId;
        this.buyerName = buyerName;
        this.lines = freezeList(lines);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public PrivateStoreSide getStoreType() {
        return storeType;
    }

    /** Seller (delivered items, received currency). Identity by role: the store-opener for {@link PrivateStoreSide#ASK ASK}, the taker for {@link PrivateStoreSide#BID BID}. */
    public long getSellerId() {
        return sellerId;
    }

    /** Optional; the platform resolves it via the {@code db-sync.character} stream. */
    public @Nullable String getSellerName() {
        return sellerName;
    }

    public long getBuyerId() {
        return buyerId;
    }

    public @Nullable String getBuyerName() {
        return buyerName;
    }

    /** Never null on read; {@code null} passed to the constructor becomes an empty list. */
    public List<TradeLine> getLines() {
        return lines;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .storeType(storeType)
                .sellerId(sellerId)
                .sellerName(sellerName)
                .buyerId(buyerId)
                .buyerName(buyerName)
                .lines(lines)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<TradeLine> freezeList(@Nullable List<TradeLine> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<TradeLine>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrivateStorePurchaseEvent)) return false;
        PrivateStorePurchaseEvent that = (PrivateStorePurchaseEvent) o;
        return sellerId == that.sellerId
                && buyerId == that.buyerId
                && Objects.equals(eventId, that.eventId)
                && storeType == that.storeType
                && Objects.equals(sellerName, that.sellerName)
                && Objects.equals(buyerName, that.buyerName)
                && Objects.equals(lines, that.lines)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, storeType, sellerId, sellerName, buyerId, buyerName, lines, metadata);
    }

    @Override
    public String toString() {
        return "PrivateStorePurchaseEvent[eventId=" + eventId
                + ", storeType=" + storeType
                + ", sellerId=" + sellerId
                + ", sellerName=" + sellerName
                + ", buyerId=" + buyerId
                + ", buyerName=" + buyerName
                + ", lines=" + lines
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private PrivateStoreSide storeType;
        private long sellerId;
        private @Nullable String sellerName;
        private long buyerId;
        private @Nullable String buyerName;
        private @Nullable List<TradeLine> lines;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder storeType(PrivateStoreSide storeType) {
            this.storeType = storeType;
            return this;
        }

        public Builder sellerId(long sellerId) {
            this.sellerId = sellerId;
            return this;
        }

        public Builder sellerName(@Nullable String sellerName) {
            this.sellerName = sellerName;
            return this;
        }

        public Builder buyerId(long buyerId) {
            this.buyerId = buyerId;
            return this;
        }

        public Builder buyerName(@Nullable String buyerName) {
            this.buyerName = buyerName;
            return this;
        }

        public Builder lines(@Nullable List<TradeLine> lines) {
            this.lines = lines;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public PrivateStorePurchaseEvent build() {
            return new PrivateStorePurchaseEvent(
                    eventId, storeType, sellerId, sellerName, buyerId, buyerName, lines, metadata);
        }
    }
}
