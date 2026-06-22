package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One service-applied line of a {@link PremiumPurchaseEvent}. Carries the
 * canonical service code, quantity, optional structured args, and the
 * per-line cost.
 *
 * <p>{@link #getCode() code} SHOULD be drawn from {@link WellKnownServices}
 * for L2-canonical services so cross-tenant dashboards can aggregate
 * consistently. Hosts MAY use private codes (e.g. {@code "bohpts:my_custom_service"})
 * for vendor-specific actions; the platform treats unknown codes as opaque.</p>
 *
 * <p>{@link #getQty() qty} is the number of identical services applied in
 * this line (e.g. {@code 3× name_change}). Defaults to {@code 1}; legacy
 * payloads without the field deserialize to {@code 1} via the getter
 * normalization. Downstream charting MUST aggregate units sold via
 * {@code sum(qty)}, not {@code count(*)}.</p>
 *
 * <p>{@link #getParams() params} carries structured arguments
 * (e.g. {@code rename}: {@code old}/{@code new}; {@code name_color_change}:
 * {@code rgb}). The map is a {@code Map<String,String>} — typed param records
 * per service code were considered and rejected; the bohpts inventory of
 * 23 well-known services would multiply DTO classes by ~25 with no win for
 * the platform consumer (whose own catalog is already weakly-typed).</p>
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

    /**
     * Canonical service code. See {@link WellKnownServices} for the L2
     * standard set; arbitrary strings are accepted for vendor-specific
     * services.
     */
    public String getCode() {
        return code;
    }

    /**
     * Quantity of identical services applied in this line. Defaults to
     * {@code 1} — legacy payloads without the field deserialize to
     * {@code null} on the underlying slot and surface as {@code 1} here.
     */
    public long getQty() {
        return qty == null ? 1L : qty;
    }

    /**
     * Optional structured arguments. Always non-null on read; {@code null}
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
        // Compare via getQty() so {qty=null} and {qty=1L} are equal — both
        // mean "one service applied" per the wire contract.
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
