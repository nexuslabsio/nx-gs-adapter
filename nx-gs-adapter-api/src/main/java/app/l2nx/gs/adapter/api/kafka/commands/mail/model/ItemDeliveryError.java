package app.l2nx.gs.adapter.api.kafka.commands.mail.model;

import app.l2nx.gs.adapter.api.kafka.commands.mail.SendMailResult;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One failed attachment line in {@link SendMailResult#getItemErrors()}; the mail is still sent without it.
 *
 * <p>Line identity is best-effort: the host's {@code MailManager} reports opaque strings that do not correlate
 * positionally with the inbound items, so {@code itemTemplateId} and {@code count} are {@code null} unless the host
 * can attribute the failure.</p>
 *
 * <p>{@code reason} is a free-form diagnostic, not a wire contract; never switch on it. {@code null} is normalized
 * to an empty string.</p>
 */
public final class ItemDeliveryError {

    private final @Nullable Long itemTemplateId;
    private final @Nullable Long count;
    private final String reason;

    public ItemDeliveryError(@Nullable Long itemTemplateId, @Nullable Long count, @Nullable String reason) {
        this.itemTemplateId = itemTemplateId;
        this.count = count;
        this.reason = reason == null ? "" : reason;
    }

    public @Nullable Long getItemTemplateId() {
        return itemTemplateId;
    }

    /** Requested size, not the delivered count (zero when this entry exists). */
    public @Nullable Long getCount() {
        return count;
    }

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
        return "ItemDeliveryError[itemTemplateId=" + itemTemplateId + ", count=" + count + ", reason=" + reason + "]";
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
