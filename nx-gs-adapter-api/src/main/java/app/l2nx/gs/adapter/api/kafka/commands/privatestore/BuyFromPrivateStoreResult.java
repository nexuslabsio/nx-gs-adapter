package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.privatestore.model.BoughtLine;
import java.util.List;
import java.util.Objects;

/**
 * What was bought and what it cost; only on OK replies, so there is no partial-result shape.
 *
 * <p>{@code paidTotalAdena = itemsTotalAdena + taxAdena}: items went to the seller, tax was burned. The constructor
 * enforces this invariant.</p>
 */
public final class BuyFromPrivateStoreResult {

    private final long itemsTotalAdena;
    private final long taxAdena;
    private final long paidTotalAdena;
    private final List<BoughtLine> bought;
    private final boolean storeClosed;
    private final long mailId;

    public BuyFromPrivateStoreResult(
            long itemsTotalAdena,
            long taxAdena,
            long paidTotalAdena,
            List<BoughtLine> bought,
            boolean storeClosed,
            long mailId) {
        if (paidTotalAdena != itemsTotalAdena + taxAdena) {
            throw new IllegalArgumentException("paidTotalAdena (" + paidTotalAdena
                    + ") must equal itemsTotalAdena + taxAdena (" + (itemsTotalAdena + taxAdena) + ")");
        }
        this.itemsTotalAdena = itemsTotalAdena;
        this.taxAdena = taxAdena;
        this.paidTotalAdena = paidTotalAdena;
        this.bought = PrivateStoreLists.freeze(bought);
        this.storeClosed = storeClosed;
        this.mailId = mailId;
    }

    public long getItemsTotalAdena() {
        return itemsTotalAdena;
    }

    /** Burned: debited from the buyer, credited to nobody. */
    public long getTaxAdena() {
        return taxAdena;
    }

    public long getPaidTotalAdena() {
        return paidTotalAdena;
    }

    public List<BoughtLine> getBought() {
        return bought;
    }

    /**
     * {@code true} when the deal emptied the store and the host closed it; the caller drops the whole store from its order book.
     */
    public boolean isStoreClosed() {
        return storeClosed;
    }

    /** Eventually consistent: the mail-read API may 404 until the asynchronous mail-ingest catches up. */
    public long getMailId() {
        return mailId;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemsTotalAdena(itemsTotalAdena)
                .taxAdena(taxAdena)
                .paidTotalAdena(paidTotalAdena)
                .bought(bought)
                .storeClosed(storeClosed)
                .mailId(mailId);
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
                && mailId == that.mailId
                && Objects.equals(bought, that.bought);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemsTotalAdena, taxAdena, paidTotalAdena, bought, Boolean.valueOf(storeClosed), mailId);
    }

    @Override
    public String toString() {
        return "BuyFromPrivateStoreResult[itemsTotalAdena=" + itemsTotalAdena
                + ", taxAdena=" + taxAdena
                + ", paidTotalAdena=" + paidTotalAdena
                + ", bought=" + bought
                + ", storeClosed=" + storeClosed
                + ", mailId=" + mailId + "]";
    }

    public static final class Builder {
        private long itemsTotalAdena;
        private long taxAdena;
        private long paidTotalAdena;
        private List<BoughtLine> bought;
        private boolean storeClosed;
        private long mailId;

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

        public Builder mailId(long mailId) {
            this.mailId = mailId;
            return this;
        }

        public BuyFromPrivateStoreResult build() {
            return new BuyFromPrivateStoreResult(
                    itemsTotalAdena, taxAdena, paidTotalAdena, bought, storeClosed, mailId);
        }
    }
}
