package app.l2nx.gs.adapter.api.kafka.events.privatetrade;

import java.util.Objects;
import java.util.UUID;

/**
 * Closed personal player-to-player trade. Successes only — cancellations
 * and rare post-confirm failures are not emitted.
 */
public final class PrivateTradeFinishedEvent {

    private final UUID eventId;
    private final UUID tradeId;
    private final TradeParty partyA;
    private final TradeParty partyB;

    public PrivateTradeFinishedEvent(UUID eventId,
                                     UUID tradeId,
                                     TradeParty partyA,
                                     TradeParty partyB) {
        this.eventId = eventId;
        this.tradeId = tradeId;
        this.partyA = partyA;
        this.partyB = partyB;
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

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .tradeId(tradeId)
                .partyA(partyA)
                .partyB(partyB);
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
                && Objects.equals(partyB, that.partyB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, tradeId, partyA, partyB);
    }

    @Override
    public String toString() {
        return "PrivateTradeFinishedEvent[eventId=" + eventId
                + ", tradeId=" + tradeId
                + ", partyA=" + partyA
                + ", partyB=" + partyB + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private UUID tradeId;
        private TradeParty partyA;
        private TradeParty partyB;

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

        public PrivateTradeFinishedEvent build() {
            return new PrivateTradeFinishedEvent(eventId, tradeId, partyA, partyB);
        }
    }
}
