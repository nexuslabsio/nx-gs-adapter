package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One item-grant line of a {@link PremiumPurchaseEvent}. Carries which item
 * was deposited into the character's inventory, in what quantity, with optional
 * host-specific metadata, and the per-line cost.
 *
 * <p>{@link #getParams() params} is a free-form {@code Map<String,String>}
 * for host-specific extension (e.g. {@code enchant=10}, {@code attribute=fire}).
 * Phase-1 bohpts datapack SKUs are unenchanted and don't use {@code params};
 * the slot is here so a future enchanted-item SKU is a non-breaking addition.</p>
 *
 * <p>{@link #getPayments() payments} is non-null and required to contain at
 * least one entry — a "free" item grant is not a purchase event and should be
 * routed through a different event family when one ships.</p>
 */
public final class PurchaseItem {

    private final long itemId;
    private final long qty;
    private final @Nullable Map<String, String> params;
    private final List<Payment> payments;

    public PurchaseItem(long itemId, long qty, @Nullable Map<String, String> params, @Nullable List<Payment> payments) {
        this.itemId = itemId;
        this.qty = qty;
        this.params = freezeMap(params);
        this.payments = freezeList(payments);
    }

    /**
     * L2 item ID granted to the character.
     */
    public long getItemId() {
        return itemId;
    }

    /**
     * Quantity of the item granted.
     *
     * <p>Soft invariant: {@code qty &gt; 0}. The constructor accepts
     * {@code 0} and negative values to keep the POJO Gson-friendly, but
     * producers MUST NOT emit non-positive grants. Consumer-side validation
     * logs and dedupes; the wire schema permits the value.</p>
     */
    public long getQty() {
        return qty;
    }

    /**
     * Optional host-specific metadata. Always non-null on read; {@code null}
     * passed to the constructor is normalized to an empty map.
     */
    public Map<String, String> getParams() {
        return params == null ? Collections.emptyMap() : params;
    }

    /**
     * Per-line cost. Non-null; producers MUST populate at least one payment.
     */
    public List<Payment> getPayments() {
        return payments == null ? Collections.emptyList() : payments;
    }

    public Builder toBuilder() {
        return new Builder().itemId(itemId).qty(qty).params(params).payments(payments);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static @Nullable Map<String, String> freezeMap(@Nullable Map<String, String> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(src));
    }

    private static List<Payment> freezeList(@Nullable List<Payment> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PurchaseItem)) return false;
        PurchaseItem that = (PurchaseItem) o;
        return itemId == that.itemId
                && qty == that.qty
                && Objects.equals(params, that.params)
                && Objects.equals(payments, that.payments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, qty, params, payments);
    }

    @Override
    public String toString() {
        return "PurchaseItem[itemId=" + itemId + ", qty=" + qty + ", params=" + params + ", payments=" + payments + "]";
    }

    public static final class Builder {
        private long itemId;
        private long qty;
        private @Nullable Map<String, String> params;
        private @Nullable List<Payment> payments;

        public Builder itemId(long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder qty(long qty) {
            this.qty = qty;
            return this;
        }

        public Builder params(@Nullable Map<String, String> params) {
            this.params = params;
            return this;
        }

        public Builder payments(@Nullable List<Payment> payments) {
            this.payments = payments;
            return this;
        }

        public PurchaseItem build() {
            return new PurchaseItem(itemId, qty, params, payments);
        }
    }
}
