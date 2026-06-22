package app.l2nx.gs.adapter.api.kafka.events.privatetrade;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Closed personal player-to-player trade. Successes only — cancellations
 * and rare post-confirm failures are not emitted.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about this trade. {@code null} when
 *   absent. Hosts MAY publish arbitrary non-canonical keys without an API
 *   release; consumers ignore keys they do not understand.</li>
 * </ul>
 */
public final class PrivateTradeFinishedEvent {

    private final UUID eventId;
    private final UUID tradeId;
    private final TradeParty partyA;
    private final TradeParty partyB;
    private final @Nullable Map<String, String> metadata;

    public PrivateTradeFinishedEvent(
            UUID eventId, UUID tradeId, TradeParty partyA, TradeParty partyB, @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.tradeId = tradeId;
        this.partyA = partyA;
        this.partyB = partyB;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    /**
     * Host-side {@code TradeList} session UUID — distinct from
     * {@link #getEventId() eventId}, stable across both parties.
     */
    public UUID getTradeId() {
        return tradeId;
    }

    public TradeParty getPartyA() {
        return partyA;
    }

    public TradeParty getPartyB() {
        return partyB;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .tradeId(tradeId)
                .partyA(partyA)
                .partyB(partyB)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrivateTradeFinishedEvent)) return false;
        PrivateTradeFinishedEvent that = (PrivateTradeFinishedEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(tradeId, that.tradeId)
                && Objects.equals(partyA, that.partyA)
                && Objects.equals(partyB, that.partyB)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, tradeId, partyA, partyB, metadata);
    }

    @Override
    public String toString() {
        return "PrivateTradeFinishedEvent[eventId=" + eventId
                + ", tradeId=" + tradeId
                + ", partyA=" + partyA
                + ", partyB=" + partyB
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private UUID tradeId;
        private TradeParty partyA;
        private TradeParty partyB;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder tradeId(UUID tradeId) {
            this.tradeId = tradeId;
            return this;
        }

        public Builder partyA(TradeParty partyA) {
            this.partyA = partyA;
            return this;
        }

        public Builder partyB(TradeParty partyB) {
            this.partyB = partyB;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public PrivateTradeFinishedEvent build() {
            return new PrivateTradeFinishedEvent(eventId, tradeId, partyA, partyB, metadata);
        }
    }
}
