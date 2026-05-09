package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO published to the {@code privatestore} family topic
 * ({@code <tenant>.gs.events.privatestore}) when a private-store deal is
 * finalized on the game thread. One event represents one transaction —
 * possibly multi-line if the counterparty atomically acquired several
 * positions in a single click.
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code occurredAt} field. Platform consumers dedupe on the {@code eventId}
 * (at-least-once delivery).</p>
 *
 * <p>{@link #getStoreType() storeType} indicates which party opened the
 * store — see {@link PrivateStoreSide} for the maker/taker direction
 * semantics.</p>
 *
 * <p>Soft invariant: {@code lines.size() >= 1}. Producers MUST NOT emit an
 * empty trade event; the wire schema permits it, the platform consumer logs
 * and dedupes rather than rejecting.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
 */
public final class PrivateStoreTradeEvent extends PrivateStoreEvent {

    private final UUID eventId;
    private final PrivateStoreSide storeType;
    private final long sellerId;
    private final @Nullable String sellerName;
    private final long buyerId;
    private final @Nullable String buyerName;
    private final List<TradeLine> lines;

    public PrivateStoreTradeEvent(UUID eventId,
                                  PrivateStoreSide storeType,
                                  long sellerId,
                                  @Nullable String sellerName,
                                  long buyerId,
                                  @Nullable String buyerName,
                                  @Nullable List<TradeLine> lines) {
        this.eventId = eventId;
        this.storeType = storeType;
        this.sellerId = sellerId;
        this.sellerName = sellerName;
        this.buyerId = buyerId;
        this.buyerName = buyerName;
        this.lines = freezeList(lines);
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the
     * occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Which side of the order book opened the store this trade closed in.
     * See {@link PrivateStoreSide} for maker/taker semantics.
     */
    public PrivateStoreSide getStoreType() {
        return storeType;
    }

    /**
     * Source-side character ID of the seller (the party that delivered items
     * and received currency). Identity-by-role, not by store-opener — for an
     * {@link PrivateStoreSide#ASK ASK} trade the seller is the store-opener;
     * for a {@link PrivateStoreSide#BID BID} trade the seller is the taker.
     */
    public long getSellerId() {
        return sellerId;
    }

    /**
     * Seller display name. Optional — host hooks may publish without it; the
     * platform resolves the name via its joined {@code db-sync.character}
     * stream.
     */
    public @Nullable String getSellerName() {
        return sellerName;
    }

    /**
     * Source-side character ID of the buyer (the party that delivered
     * currency and received items).
     */
    public long getBuyerId() {
        return buyerId;
    }

    /**
     * Buyer display name. Optional.
     */
    public @Nullable String getBuyerName() {
        return buyerName;
    }

    /**
     * Per-position breakdown of the trade. Always non-null on read;
     * {@code null} passed to the constructor is normalized to an empty list.
     * Soft invariant: producers populate at least one line.
     */
    public List<TradeLine> getLines() {
        return lines;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .storeType(storeType)
                .sellerId(sellerId)
                .sellerName(sellerName)
                .buyerId(buyerId)
                .buyerName(buyerName)
                .lines(lines);
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
        if (!(o instanceof PrivateStoreTradeEvent)) return false;
        PrivateStoreTradeEvent that = (PrivateStoreTradeEvent) o;
        return sellerId == that.sellerId
                && buyerId == that.buyerId
                && Objects.equals(eventId, that.eventId)
                && storeType == that.storeType
                && Objects.equals(sellerName, that.sellerName)
                && Objects.equals(buyerName, that.buyerName)
                && Objects.equals(lines, that.lines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, storeType, sellerId, sellerName, buyerId, buyerName, lines);
    }

    @Override
    public String toString() {
        return "PrivateStoreTradeEvent[eventId=" + eventId
                + ", storeType=" + storeType
                + ", sellerId=" + sellerId
                + ", sellerName=" + sellerName
                + ", buyerId=" + buyerId
                + ", buyerName=" + buyerName
                + ", lines=" + lines + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private PrivateStoreSide storeType;
        private long sellerId;
        private @Nullable String sellerName;
        private long buyerId;
        private @Nullable String buyerName;
        private @Nullable List<TradeLine> lines;

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

        public PrivateStoreTradeEvent build() {
            return new PrivateStoreTradeEvent(
                    eventId, storeType, sellerId, sellerName, buyerId, buyerName, lines);
        }
    }
}
