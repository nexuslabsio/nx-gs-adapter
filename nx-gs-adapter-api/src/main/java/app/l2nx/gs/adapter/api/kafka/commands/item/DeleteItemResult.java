package app.l2nx.gs.adapter.api.kafka.commands.item;

import java.util.Objects;

/**
 * Echoes what was actually deleted; may be less than requested if the live stack shrank before execution.
 */
public final class DeleteItemResult {

    private final Long itemId;
    private final Long countDeleted;
    private final boolean fullyDeleted;

    public DeleteItemResult(Long itemId, Long countDeleted, boolean fullyDeleted) {
        if (itemId == null) {
            throw new IllegalArgumentException("itemId is required");
        }
        if (countDeleted == null) {
            throw new IllegalArgumentException("countDeleted is required");
        }
        if (countDeleted < 0L) {
            throw new IllegalArgumentException("countDeleted must be non-negative (got " + countDeleted + ")");
        }
        this.itemId = itemId;
        this.countDeleted = countDeleted;
        this.fullyDeleted = fullyDeleted;
    }

    public Long getItemId() {
        return itemId;
    }

    /** May be less than the requested count: the handler clamps to the live stack. */
    public Long getCountDeleted() {
        return countDeleted;
    }

    /** {@code true} when the instance is gone, {@code false} when it was only decremented. */
    public boolean isFullyDeleted() {
        return fullyDeleted;
    }

    public Builder toBuilder() {
        return new Builder().itemId(itemId).countDeleted(countDeleted).fullyDeleted(fullyDeleted);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteItemResult)) return false;
        DeleteItemResult that = (DeleteItemResult) o;
        return fullyDeleted == that.fullyDeleted
                && Objects.equals(itemId, that.itemId)
                && Objects.equals(countDeleted, that.countDeleted);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, countDeleted, fullyDeleted);
    }

    @Override
    public String toString() {
        return "DeleteItemResult[itemId=" + itemId
                + ", countDeleted=" + countDeleted
                + ", fullyDeleted=" + fullyDeleted + "]";
    }

    public static final class Builder {
        private Long itemId;
        private Long countDeleted;
        private boolean fullyDeleted;

        public Builder itemId(Long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder countDeleted(Long countDeleted) {
            this.countDeleted = countDeleted;
            return this;
        }

        public Builder fullyDeleted(boolean fullyDeleted) {
            this.fullyDeleted = fullyDeleted;
            return this;
        }

        public DeleteItemResult build() {
            return new DeleteItemResult(itemId, countDeleted, fullyDeleted);
        }
    }
}
