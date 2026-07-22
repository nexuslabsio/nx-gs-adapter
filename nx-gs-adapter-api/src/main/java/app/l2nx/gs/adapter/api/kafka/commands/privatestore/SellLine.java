package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.Objects;

/**
 * One sell-offer line of a private-store "open" command: an inventory item
 * stack and the per-unit adena price the character is asking for it.
 *
 * <p><b>Identity.</b> {@link #getItemId() itemId} is the character's
 * inventory <em>instance</em> object-id (NOT a catalog item-template id) —
 * the item already exists in the seller's inventory when the store opens.</p>
 *
 * <p><b>Required fields.</b> All three fields are REQUIRED — the constructor
 * enforces {@code count > 0} and {@code priceAdena >= 0} via
 * {@link IllegalArgumentException} for programmatic construction. Wire-path
 * deserialization bypasses the constructor — the handler re-checks and emits
 * {@code VALIDATION_FAILED}.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
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

    /**
     * Inventory instance object-id of the item stack being offered.
     */
    public int getItemId() {
        return itemId;
    }

    /**
     * Quantity of the stack offered for sale. REQUIRED, MUST be positive.
     */
    public long getCount() {
        return count;
    }

    /**
     * Adena price asked per unit — the engine charges
     * {@code count * priceAdena} for the whole stack (L2 private-store
     * semantics). REQUIRED, MUST be non-negative ({@code 0} is a valid
     * give-away price).
     */
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
