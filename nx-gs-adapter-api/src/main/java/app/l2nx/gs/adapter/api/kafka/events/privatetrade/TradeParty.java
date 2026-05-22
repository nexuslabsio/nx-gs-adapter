package app.l2nx.gs.adapter.api.kafka.events.privatetrade;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One symmetric side of a closed personal trade. {@link #getItems() items}
 * lists what this side gave up; empty list = gift recipient.
 */
public final class TradeParty {

    private final long charId;
    private final List<TradeItemMovement> items;

    public TradeParty(long charId,
                      @Nullable List<TradeItemMovement> items) {
        this.charId = charId;
        this.items = freezeList(items);
    }

    public long getCharId() {
        return charId;
    }

    public List<TradeItemMovement> getItems() {
        return items;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .items(items);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<TradeItemMovement> freezeList(@Nullable List<TradeItemMovement> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<TradeItemMovement>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TradeParty)) return false;
        TradeParty that = (TradeParty) o;
        return charId == that.charId
                && Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, items);
    }

    @Override
    public String toString() {
        return "TradeParty[charId=" + charId
                + ", items=" + items + "]";
    }

    public static final class Builder {
        private long charId;
        private @Nullable List<TradeItemMovement> items;

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder items(@Nullable List<TradeItemMovement> items) {
            this.items = items;
            return this;
        }

        public TradeParty build() {
            return new TradeParty(charId, items);
        }
    }
}
