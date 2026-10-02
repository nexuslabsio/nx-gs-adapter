package app.l2nx.gs.adapter.api.kafka.commands.privatestore.model;

import java.util.Objects;

/** A {@link SellLine} the host rejected when opening the store. */
public final class DroppedLine {

    private final int itemId;
    private final String reason;

    public DroppedLine(int itemId, String reason) {
        if (reason == null) {
            throw new IllegalArgumentException("reason is required");
        }
        this.itemId = itemId;
        this.reason = reason;
    }

    public int getItemId() {
        return itemId;
    }

    /**
     * Open {@code UPPER_SNAKE_CASE} token (known: {@code NOT_FOUND}, {@code NOT_TRADEABLE}, {@code ITEM_BLOCKED},
     * {@code EQUIPPED}, {@code BAD_COUNT}, {@code PRICE_OVERFLOW}, {@code REJECTED}); consumers MUST tolerate unknown tokens.
     */
    public String getReason() {
        return reason;
    }

    public Builder toBuilder() {
        return new Builder().itemId(itemId).reason(reason);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DroppedLine)) return false;
        DroppedLine that = (DroppedLine) o;
        return itemId == that.itemId && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, reason);
    }

    @Override
    public String toString() {
        return "DroppedLine[itemId=" + itemId + ", reason=" + reason + "]";
    }

    public static final class Builder {
        private int itemId;
        private String reason;

        public Builder itemId(int itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public DroppedLine build() {
            return new DroppedLine(itemId, reason);
        }
    }
}
