package app.l2nx.gs.adapter.api.kafka.commands.privatestore.model;

import java.util.Objects;

/** {@code itemId} is the inventory instance object-id (NOT a catalog template id). */
public final class SellLine {

    private final int itemId;
    private final long count;
    private final long priceAdena;

    public SellLine(int itemId, long count, long priceAdena) {
        if (count <= 0L) {
            throw new IllegalArgumentException("count must be positive (got " + count + ")");
        }
        if (priceAdena < 0L) {
            throw new IllegalArgumentException("priceAdena must be non-negative (got " + priceAdena + ")");
        }
        this.itemId = itemId;
        this.count = count;
        this.priceAdena = priceAdena;
    }

    public int getItemId() {
        return itemId;
    }

    public long getCount() {
        return count;
    }

    /** The engine charges {@code count * priceAdena} for the stack; {@code 0} is a valid give-away price. */
    public long getPriceAdena() {
        return priceAdena;
    }

    public Builder toBuilder() {
        return new Builder().itemId(itemId).count(count).priceAdena(priceAdena);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SellLine)) return false;
        SellLine that = (SellLine) o;
        return itemId == that.itemId && count == that.count && priceAdena == that.priceAdena;
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, count, priceAdena);
    }

    @Override
    public String toString() {
        return "SellLine[itemId=" + itemId + ", count=" + count + ", priceAdena=" + priceAdena + "]";
    }

    public static final class Builder {
        private int itemId;
        private long count;
        private long priceAdena;

        public Builder itemId(int itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder count(long count) {
            this.count = count;
            return this;
        }

        public Builder priceAdena(long priceAdena) {
            this.priceAdena = priceAdena;
            return this;
        }

        public SellLine build() {
            return new SellLine(itemId, count, priceAdena);
        }
    }
}
