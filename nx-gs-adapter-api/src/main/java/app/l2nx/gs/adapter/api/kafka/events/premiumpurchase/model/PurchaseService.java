package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.model;

import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One service-applied line of a {@link PremiumPurchaseEvent}.
 * <p>{@link #getCode() code} SHOULD come from {@link WellKnownServices} so cross-tenant dashboards aggregate consistently; hosts MAY use private codes (e.g. {@code "myhost:my_custom_service"}), which the platform treats as opaque.
 * <p>{@link #getQty() qty} is the number of identical services applied; aggregate units sold via {@code sum(qty)}, not {@code count(*)}.
 * <p>{@link #getParams() params} is a plain {@code Map<String,String>} of per-service arguments (e.g. {@code rename}: {@code old}/{@code new}).
 */
public final class PurchaseService {

    private final String code;
    private final @Nullable Long qty;
    private final @Nullable Map<String, String> params;
    private final List<Payment> payments;

    public PurchaseService(
            String code, @Nullable Long qty, @Nullable Map<String, String> params, @Nullable List<Payment> payments) {
        this.code = code;
        this.qty = qty;
        this.params = freezeMap(params);
        this.payments = freezeList(payments);
    }

    public String getCode() {
        return code;
    }

    /** Defaults to {@code 1}; legacy payloads without the field read as {@code 1}. */
    public long getQty() {
        return qty == null ? 1L : qty;
    }

    /** Never null on read; {@code null} passed to the constructor becomes an empty map. */
    public Map<String, String> getParams() {
        return params == null ? Collections.emptyMap() : params;
    }

    /** Producers MUST populate at least one payment. */
    public List<Payment> getPayments() {
        return payments == null ? Collections.emptyList() : payments;
    }

    public Builder toBuilder() {
        return new Builder().code(code).qty(getQty()).params(params).payments(payments);
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
        if (!(o instanceof PurchaseService)) return false;
        PurchaseService that = (PurchaseService) o;
        // getQty() so {qty=null} and {qty=1L} compare equal
        return getQty() == that.getQty()
                && Objects.equals(code, that.code)
                && Objects.equals(params, that.params)
                && Objects.equals(payments, that.payments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, getQty(), params, payments);
    }

    @Override
    public String toString() {
        return "PurchaseService[code=" + code + ", qty=" + getQty() + ", params=" + params + ", payments=" + payments
                + "]";
    }

    public static final class Builder {
        private String code;
        private long qty = 1L;
        private @Nullable Map<String, String> params;
        private @Nullable List<Payment> payments;

        public Builder code(String code) {
            this.code = code;
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

        public PurchaseService build() {
            return new PurchaseService(code, qty, params, payments);
        }
    }
}
