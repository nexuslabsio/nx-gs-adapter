package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase;

import java.util.Objects;

/**
 * One currency-line of payment for a single {@link PurchaseItem} or
 * {@link PurchaseService}. Multi-currency lines are first-class — a single
 * purchase line can require, e.g., 20 Coin-of-Luck plus 10M Adena
 * (Giant Codex Mastery on the bohpts custom shop).
 *
 * <p>{@link #getCurrencyItemId()} is the raw L2 item ID
 * (e.g. {@code 4037} for Coin of Luck, {@code 57} for Adena). The platform
 * maps id → human-readable currency name via its own catalog; the wire stays
 * honest about what's actually being charged.</p>
 */
public final class Payment {

    private final long currencyItemId;
    private final long qty;

    public Payment(long currencyItemId, long qty) {
        this.currencyItemId = currencyItemId;
        this.qty = qty;
    }

    /**
     * L2 item ID acting as the currency for this payment line.
     */
    public long getCurrencyItemId() {
        return currencyItemId;
    }

    /**
     * Quantity of the currency item charged.
     *
     * <p>Soft invariant: {@code qty &gt; 0}. The constructor accepts
     * {@code 0} and negative values to keep the POJO Gson-friendly, but
     * producers MUST NOT emit non-positive payments — a "free" line is not
     * a purchase. Consumer-side validation logs and dedupes; the wire schema
     * permits the value.</p>
     */
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
