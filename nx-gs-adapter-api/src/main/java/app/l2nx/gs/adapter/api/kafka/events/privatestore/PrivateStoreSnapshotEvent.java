package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO published to the {@code privatestore} family topic
 * ({@code <tenant>.gs.events.privatestore}) by a host-managed daemon when
 * the order book for one {@code (itemTemplateId, side)} pair has changed
 * since the previous tick. Carries the full per-pair order book at the
 * snapshot tick — NOT a delta.
 *
 * <p><b>One event per pair, per tick, per change.</b> The host iterates open
 * private stores on each tick, groups offers by {@code (itemTemplateId, side)},
 * hashes the canonical-sorted offer list, and publishes only pairs whose hash
 * differs from the previously emitted hash. Unchanged pairs are NOT published
 * — the platform consumer keeps the last-known state and only updates on
 * receipt.</p>
 *
 * <p><b>Tombstones.</b> When a previously-tracked pair has no offers at the
 * current tick (every store closed or every offer drained), the host emits one
 * event with {@link #getOffers() offers} {@code = []} so the consumer
 * observes the book closing, then drops the pair from its internal tracker.
 * Repeated empty ticks for the same pair are NOT republished.</p>
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The wire timestamp is
 * encoded in the upper 48 bits. Platform consumers dedupe on the
 * {@code eventId} (at-least-once delivery) and order within
 * {@code (itemTemplateId, side)} by the embedded timestamp.</p>
 *
 * <p><b>Offer ordering on the wire is unspecified for consumers</b> — they
 * MUST re-sort if they need a stable order — but producers SHOULD canonical-sort
 * by {@code (unitPrice ASC, traderId ASC, enchantLevel ASC)} BEFORE hashing,
 * to keep change-detection from firing on insertion-order noise.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
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

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the
     * snapshot occurrence timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * The item template this order-book snapshot describes. Used as the
     * Kafka partition key (8-byte big-endian), so all updates for the same
     * item template land on the same partition for ordered consumption /
     * topic compaction.
     */
    public long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Which side of the order book this snapshot represents — see
     * {@link PrivateStoreSide}. {@link PrivateStoreSide#ASK ASK} aggregates
     * offers from SELL stores; {@link PrivateStoreSide#BID BID} aggregates
     * offers from BUY stores.
     */
    public PrivateStoreSide getSide() {
        return side;
    }

    /**
     * Current open offers for this {@code (itemTemplateId, side)} pair.
     * Always non-null on read; {@code null} passed to the constructor is
     * normalized to an empty list. An empty list is meaningful — see the
     * class-level Javadoc for tombstone semantics.
     */
    public List<Offer> getOffers() {
        return offers;
    }

    /**
     * Optional open string→string map of build-agnostic attributes about this
     * snapshot. {@code null} when absent. Hosts MAY add
     * arbitrary keys without an API release; consumers ignore keys they do not
     * understand.
     */
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
