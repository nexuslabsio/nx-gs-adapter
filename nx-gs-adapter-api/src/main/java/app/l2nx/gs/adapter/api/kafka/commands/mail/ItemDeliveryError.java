package app.l2nx.gs.adapter.api.kafka.commands.mail;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One per-item failure entry in {@link SendMailPayload#getItemErrors()}.
 * Carries an optional inbound-line hint plus a non-null host-supplied reason
 * string so the platform can render a partial-success report.
 *
 * <p>Emitted by the host when an attachment line fails to materialize during
 * mail composition (e.g. the template id does not exist in the host's item
 * catalog, or the host's attachment-creation pipeline rejects the line for
 * other reasons). The mail itself is still sent — only specific attachment
 * lines are dropped — so partial failures surface as
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult#success(Object)
 * success} with a non-empty {@code itemErrors}, NOT as an error envelope.</p>
 *
 * <p><b>Identity is best-effort.</b> The bohpts {@code MailManager} reports
 * per-line failures as opaque human-readable strings without machine-readable
 * line identity, and the failure list does NOT correlate positionally with
 * the inbound items list (errors only appear for lines that failed). So
 * {@link #getItemTemplateId() itemTemplateId} and {@link #getCount() count}
 * are {@code @Nullable} on the wire — populated only when the host can
 * confidently attribute the failure to a specific inbound line. The
 * {@link #getReason() reason} string is the always-present diagnostic.</p>
 *
 * <p><b>Reason format.</b> Free-form host-supplied diagnostic. Stable enough to
 * surface in operator-facing UIs but NOT a wire contract — the platform MUST
 * NOT switch on the string. Use the absence/presence of an entry as the
 * machine-readable "this line failed" signal.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names. {@code reason}
 * normalized to empty string at construction when {@code null} is passed,
 * so getters and {@code equals}/{@code hashCode}/{@code toString} agree.</p>
 */
public final class ItemDeliveryError {

    private final @Nullable Long itemTemplateId;
    private final @Nullable Long count;
    private final String reason;

    public ItemDeliveryError(@Nullable Long itemTemplateId,
                             @Nullable Long count,
                             @Nullable String reason) {
        this.itemTemplateId = itemTemplateId;
        this.count = count;
        this.reason = reason == null ? "" : reason;
    }

    /**
     * Catalog item-template id of the failed line when the host can attribute
     * the failure to a specific inbound {@link MailItem}; otherwise
     * {@code null}.
     */
    public @Nullable Long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Requested stack size of the failed line when known; otherwise
     * {@code null}. Carried for parity with the inbound request line — not
     * the count actually delivered (which is zero when this entry is present).
     */
    public @Nullable Long getCount() {
        return count;
    }

    /**
     * Host-supplied diagnostic reason. Always non-null; {@code null} passed
     * to the constructor is normalized to an empty string. Free-form — NOT a
     * wire-stable discriminator.
     */
    public String getReason() {
        return reason;
    }

    public Builder toBuilder() {
        return new Builder().itemTemplateId(itemTemplateId).count(count).reason(reason);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemDeliveryError)) return false;
        ItemDeliveryError that = (ItemDeliveryError) o;
        return Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(count, that.count)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, count, reason);
    }

    @Override
    public String toString() {
        return "ItemDeliveryError[itemTemplateId=" + itemTemplateId
                + ", count=" + count
                + ", reason=" + reason + "]";
    }

    public static final class Builder {
        private @Nullable Long itemTemplateId;
        private @Nullable Long count;
        private @Nullable String reason;

        public Builder itemTemplateId(@Nullable Long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder count(@Nullable Long count) {
            this.count = count;
            return this;
        }

        public Builder reason(@Nullable String reason) {
            this.reason = reason;
            return this;
        }

        public ItemDeliveryError build() {
            return new ItemDeliveryError(itemTemplateId, count, reason);
        }
    }
}
