package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.model;

import java.util.Objects;

/**
 * One currency line of payment for a {@link PurchaseItem} or {@link PurchaseService}; multi-currency lines are first-class (e.g. 20 Coin of Luck plus 10M Adena).
 * <p>{@link #getCurrencyItemId()} is the raw L2 item id ({@code 4037} Coin of Luck, {@code 57} Adena); the platform maps it to a name.
 */
public final class Payment {

    private final long currencyItemId;
    private final long qty;

    public Payment(long currencyItemId, long qty) {
        this.currencyItemId = currencyItemId;
        this.qty = qty;
    }

    public long getCurrencyItemId() {
        return currencyItemId;
    }

    /** Soft invariant: {@code qty &gt; 0}. The constructor accepts {@code 0} / negatives to stay Gson-friendly, but producers MUST NOT emit them; consumers log and dedupe. */
    public long getQty() {
        return qty;
    }

    public Builder toBuilder() {
        return new Builder().currencyItemId(currencyItemId).qty(qty);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Payment)) return false;
        Payment that = (Payment) o;
        return currencyItemId == that.currencyItemId && qty == that.qty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currencyItemId, qty);
    }

    @Override
    public String toString() {
        return "Payment[currencyItemId=" + currencyItemId + ", qty=" + qty + "]";
    }

    public static final class Builder {
        private long currencyItemId;
        private long qty;

        public Builder currencyItemId(long currencyItemId) {
            this.currencyItemId = currencyItemId;
            return this;
        }

        public Builder qty(long qty) {
            this.qty = qty;
            return this;
        }

        public Payment build() {
            return new Payment(currencyItemId, qty);
        }
    }
}
