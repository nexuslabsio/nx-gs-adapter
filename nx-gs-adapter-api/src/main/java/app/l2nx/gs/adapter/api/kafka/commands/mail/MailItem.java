package app.l2nx.gs.adapter.api.kafka.commands.mail;

import java.util.Objects;

/**
 * One attachment line of a {@link SendMailCommand}: a catalog item template
 * id and the quantity to be created and attached to the outgoing system mail.
 *
 * <p><b>Identity.</b> {@link #getItemTemplateId() itemTemplateId} is the
 * <em>catalog</em> item template id (NOT the per-instance object-id used by
 * {@link app.l2nx.gs.adapter.api.kafka.commands.item.DeleteItemCommand
 * DeleteItemCommand}) — when a mail is sent, the items do not yet exist; the
 * game-server materializes a fresh stack from the template at send time.</p>
 *
 * <p><b>Quantity semantics.</b> {@link #getCount() count} is the size of the
 * stack to attach. Builder defaults to {@code 1}. {@code count <= 0} is rejected
 * at construction (programmatic use); on the wire path the handler MUST emit
 * {@link app.l2nx.gs.adapter.api.kafka.commands.ErrorCode#VALIDATION_FAILED
 * VALIDATION_FAILED} when {@code count} is missing or non-positive.</p>
 *
 * <p><b>Required fields.</b> Both {@code itemTemplateId} and {@code count} are
 * REQUIRED. The constructor enforces non-null + positive {@code count} via
 * {@link IllegalArgumentException} for programmatic construction (tests,
 * host-side replays). Wire-path Gson bypasses the constructor — handler-side
 * null-checking is the wire-validation gate.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class MailItem {

    private final Long itemTemplateId;
    private final Long count;

    public MailItem(Long itemTemplateId, Long count) {
        if (itemTemplateId == null) {
            throw new IllegalArgumentException("itemTemplateId is required");
        }
        if (count == null) {
            throw new IllegalArgumentException("count is required");
        }
        if (count <= 0L) {
            throw new IllegalArgumentException("count must be positive (got " + count + ")");
        }
        this.itemTemplateId = itemTemplateId;
        this.count = count;
    }

    /**
     * Catalog item-template id from which the attached stack is created.
     * REQUIRED. Handler MUST emit {@code VALIDATION_FAILED} when the wire
     * payload omits this field (boxed {@code Long} surfaces missing wire data
     * as {@code null}).
     */
    public Long getItemTemplateId() {
        return itemTemplateId;
    }

    /**
     * Stack size of the attachment. REQUIRED, MUST be positive. Builder
     * defaults to {@code 1}; the wire MUST carry the field explicitly.
     * Handler MUST emit {@code VALIDATION_FAILED} on missing or non-positive
     * values.
     */
    public Long getCount() {
        return count;
    }

    public Builder toBuilder() {
        return new Builder().itemTemplateId(itemTemplateId).count(count);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MailItem)) return false;
        MailItem that = (MailItem) o;
        return Objects.equals(itemTemplateId, that.itemTemplateId)
                && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, count);
    }

    @Override
    public String toString() {
        return "MailItem[itemTemplateId=" + itemTemplateId
                + ", count=" + count + "]";
    }

    public static final class Builder {
        private Long itemTemplateId;
        private Long count = 1L;

        public Builder itemTemplateId(Long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        /**
         * Override the default count of 1.
         */
        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public MailItem build() {
            return new MailItem(itemTemplateId, count);
        }
    }
}
