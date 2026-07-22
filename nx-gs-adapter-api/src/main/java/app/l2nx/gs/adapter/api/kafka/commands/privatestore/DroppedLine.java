package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.Objects;

/**
 * One line of a {@link StartPrivateStoreResult#getDropped() dropped} report:
 * a requested {@link SellLine} the host rejected when opening the store, with
 * a short host-supplied reason.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
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

    /**
     * Inventory instance object-id of the rejected {@link SellLine}.
     */
    public int getItemId() {
        return itemId;
    }

    /**
     * Host-supplied rejection reason as a stable {@code UPPER_SNAKE_CASE} token.
     * Open enum: known tokens are {@code NOT_FOUND}, {@code NOT_TRADEABLE},
     * {@code ITEM_BLOCKED}, {@code EQUIPPED}, {@code BAD_COUNT},
     * {@code PRICE_OVERFLOW}, and {@code REJECTED}. The set is not closed —
     * consumers MUST tolerate unknown tokens (treat as a generic rejection).
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
