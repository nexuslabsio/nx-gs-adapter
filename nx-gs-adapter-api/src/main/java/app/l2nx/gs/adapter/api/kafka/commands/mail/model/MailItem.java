package app.l2nx.gs.adapter.api.kafka.commands.mail.model;

import java.util.Objects;

/**
 * One attachment line: catalog item template id (NOT an instance object-id; the stack is materialized at send time)
 * and stack size. {@code count} must be positive; Gson bypasses the constructor, so the handler must emit
 * {@code VALIDATION_FAILED} on missing or non-positive values.
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

    public Long getItemTemplateId() {
        return itemTemplateId;
    }

    /** The builder defaults to {@code 1}; the wire must carry it explicitly. */
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
        return Objects.equals(itemTemplateId, that.itemTemplateId) && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemTemplateId, count);
    }

    @Override
    public String toString() {
        return "MailItem[itemTemplateId=" + itemTemplateId + ", count=" + count + "]";
    }

    public static final class Builder {
        private Long itemTemplateId;
        private Long count = 1L;

        public Builder itemTemplateId(Long itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public MailItem build() {
            return new MailItem(itemTemplateId, count);
        }
    }
}
