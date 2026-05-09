package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * One service-applied line of a {@link PremiumPurchaseEvent}. Carries the
 * canonical service code, optional structured args, and the per-line cost.
 *
 * <p>{@link #getCode() code} SHOULD be drawn from {@link WellKnownServices}
 * for L2-canonical services so cross-tenant dashboards can aggregate
 * consistently. Hosts MAY use private codes (e.g. {@code "bohpts:my_custom_service"})
 * for vendor-specific actions; the platform treats unknown codes as opaque.</p>
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
    private final @Nullable Map<String, String> params;
    private final List<Payment> payments;

    public PurchaseService(String code,
                           @Nullable Map<String, String> params,
                           @Nullable List<Payment> payments) {
        this.code = code;
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
        return new Builder().code(code).params(params).payments(payments);
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
        return Objects.equals(code, that.code)
                && Objects.equals(params, that.params)
                && Objects.equals(payments, that.payments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, params, payments);
    }

    @Override
    public String toString() {
        return "PurchaseService[code=" + code + ", params=" + params
                + ", payments=" + payments + "]";
    }

    public static final class Builder {
        private String code;
        private @Nullable Map<String, String> params;
        private @Nullable List<Payment> payments;

        public Builder code(String code) {
            this.code = code;
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
            return new PurchaseService(code, params, payments);
        }
    }
}
