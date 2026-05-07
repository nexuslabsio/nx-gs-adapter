package app.l2nx.gs.adapter.api.kafka.commands.mail;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Success payload of {@link SendMailCommand}: the set of created mail-message
 * primary keys plus an optional per-attachment-line error report for
 * partial-success cases.
 *
 * <p><b>Multi-mail batching.</b> When {@link SendMailCommand#getItems()
 * items.size()} exceeds the host's per-mail attachment cap (host-defined; in
 * the bohpts fork it is {@code Config.MAIL_MAX_ATTACHMENTS}), the host splits
 * the request into multiple outgoing system mails titled
 * {@code "<title> 1/N"} … {@code "<title> N/N"} and returns one row in
 * {@link #getCreatedMailIds() createdMailIds} per resulting mail. Single-mail
 * sends produce a single-element list.</p>
 *
 * <p><b>Partial item-creation failures.</b> When the host's attachment pipeline
 * rejects a specific line (template id unknown, item factory fails, …), the
 * mail is still sent without that attachment and the failure is reported as an
 * entry in {@link #getItemErrors() itemErrors}. The reply envelope remains
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult#success(Object)
 * success} — the platform must inspect {@code itemErrors} to detect partial
 * failures. This preserves the legacy bohpts semantic (mail delivery is the
 * primary contract; per-item failures are diagnostic).</p>
 *
 * <p><b>Empty-list semantics.</b> Both lists are non-null on read; {@code null}
 * passed to the constructor is normalized to {@link Collections#emptyList()}.
 * An empty {@code itemErrors} list signals "all attachments materialized
 * successfully". An empty {@code createdMailIds} list paired with an error
 * envelope signals "no mail created" (e.g. character not found); on success
 * envelopes {@code createdMailIds} is always non-empty.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; defensive copy in
 * constructor; unmodifiable list views from getters.</p>
 */
public final class SendMailPayload {

    private final List<Long> createdMailIds;
    private final List<ItemDeliveryError> itemErrors;

    public SendMailPayload(@Nullable List<Long> createdMailIds,
                           @Nullable List<ItemDeliveryError> itemErrors) {
        this.createdMailIds = MailLists.freeze(createdMailIds);
        this.itemErrors = MailLists.freeze(itemErrors);
    }

    /**
     * Primary keys of the created mail rows. Non-null; empty when the command
     * is replying with a non-success envelope. Multi-element when the host
     * batched the attachments across multiple mails (see class Javadoc).
     */
    public List<Long> getCreatedMailIds() {
        return createdMailIds;
    }

    /**
     * Per-attachment-line failures encountered during mail composition.
     * Non-null; empty when all requested attachments materialized successfully.
     * Non-empty {@code itemErrors} on a success envelope means partial
     * delivery — the mail was sent without those attachments.
     */
    public List<ItemDeliveryError> getItemErrors() {
        return itemErrors;
    }

    public Builder toBuilder() {
        return new Builder().createdMailIds(createdMailIds).itemErrors(itemErrors);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SendMailPayload)) return false;
        SendMailPayload that = (SendMailPayload) o;
        return Objects.equals(createdMailIds, that.createdMailIds)
                && Objects.equals(itemErrors, that.itemErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(createdMailIds, itemErrors);
    }

    @Override
    public String toString() {
        return "SendMailPayload[createdMailIds=" + createdMailIds
                + ", itemErrors=" + itemErrors + "]";
    }

    public static final class Builder {
        private @Nullable List<Long> createdMailIds;
        private @Nullable List<ItemDeliveryError> itemErrors;

        public Builder createdMailIds(@Nullable List<Long> createdMailIds) {
            this.createdMailIds = createdMailIds;
            return this;
        }

        public Builder itemErrors(@Nullable List<ItemDeliveryError> itemErrors) {
            this.itemErrors = itemErrors;
            return this;
        }

        public SendMailPayload build() {
            return new SendMailPayload(createdMailIds, itemErrors);
        }
    }
}
