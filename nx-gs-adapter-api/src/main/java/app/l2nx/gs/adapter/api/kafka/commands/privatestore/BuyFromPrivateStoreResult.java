package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.List;
import java.util.Objects;

/**
 * Success payload of {@link BuyFromPrivateStoreCommand} — what was bought and
 * what it cost. Only ever carried on an OK reply: a rejected purchase moves
 * nothing and charges nothing, so there is no partial-result shape.
 *
 * <p><b>Money.</b> {@code paidTotalAdena = itemsTotalAdena + taxAdena}, where
 * {@code itemsTotalAdena} went to the seller and {@code taxAdena} was burned —
 * debited from the buyer and credited to nobody.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class BuyFromPrivateStoreResult {

    private final long itemsTotalAdena;
    private final long taxAdena;
    private final long paidTotalAdena;
    private final List<BoughtLine> bought;
    private final boolean storeClosed;

    public BuyFromPrivateStoreResult(
            long itemsTotalAdena, long taxAdena, long paidTotalAdena, List<BoughtLine> bought, boolean storeClosed) {
        this.itemsTotalAdena = itemsTotalAdena;
        this.taxAdena = taxAdena;
        this.paidTotalAdena = paidTotalAdena;
        this.bought = PrivateStoreLists.freeze(bought);
        this.storeClosed = storeClosed;
    }

    /**
     * Lot price total credited to the seller.
     */
    public long getItemsTotalAdena() {
        return itemsTotalAdena;
    }

    /**
     * Burned surcharge — debited from the buyer on top of
     * {@link #getItemsTotalAdena()} and credited to no one.
     */
    public long getTaxAdena() {
        return taxAdena;
    }

    /**
     * Total debited from the buyer.
     */
    public long getPaidTotalAdena() {
        return paidTotalAdena;
    }

    /**
     * The executed lots. Immutable on read.
     */
    public List<BoughtLine> getBought() {
        return bought;
    }

    /**
     * {@code true} when this deal emptied the seller's store and the host
     * closed it — the caller drops the whole store from its order book instead
     * of decrementing the bought lots.
     */
    public boolean isStoreClosed() {
        return storeClosed;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemsTotalAdena(itemsTotalAdena)
                .taxAdena(taxAdena)
                .paidTotalAdena(paidTotalAdena)
                .bought(bought)
                .storeClosed(storeClosed);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BuyFromPrivateStoreResult)) return false;
        BuyFromPrivateStoreResult that = (BuyFromPrivateStoreResult) o;
        return itemsTotalAdena == that.itemsTotalAdena
                && taxAdena == that.taxAdena
                && paidTotalAdena == that.paidTotalAdena
                && storeClosed == that.storeClosed
                && Objects.equals(bought, that.bought);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemsTotalAdena, taxAdena, paidTotalAdena, bought, Boolean.valueOf(storeClosed));
    }

    @Override
    public String toString() {
        return "BuyFromPrivateStoreResult[itemsTotalAdena=" + itemsTotalAdena
                + ", taxAdena=" + taxAdena
                + ", paidTotalAdena=" + paidTotalAdena
                + ", bought=" + bought
                + ", storeClosed=" + storeClosed + "]";
    }

    public static final class Builder {
        private long itemsTotalAdena;
        private long taxAdena;
        private long paidTotalAdena;
        private List<BoughtLine> bought;
        private boolean storeClosed;

        public Builder itemsTotalAdena(long itemsTotalAdena) {
            this.itemsTotalAdena = itemsTotalAdena;
            return this;
        }

        public Builder taxAdena(long taxAdena) {
            this.taxAdena = taxAdena;
            return this;
        }

        public Builder paidTotalAdena(long paidTotalAdena) {
            this.paidTotalAdena = paidTotalAdena;
            return this;
        }

        public Builder bought(List<BoughtLine> bought) {
            this.bought = bought;
            return this;
        }

        public Builder storeClosed(boolean storeClosed) {
            this.storeClosed = storeClosed;
            return this;
        }

        public BuyFromPrivateStoreResult build() {
            return new BuyFromPrivateStoreResult(itemsTotalAdena, taxAdena, paidTotalAdena, bought, storeClosed);
        }
    }
}
