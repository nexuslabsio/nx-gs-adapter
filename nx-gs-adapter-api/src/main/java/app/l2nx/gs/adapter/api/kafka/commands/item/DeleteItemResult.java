package app.l2nx.gs.adapter.api.kafka.commands.item;

import java.util.Objects;

/**
 * Success payload of {@link DeleteItemCommand}. Echoes what was actually
 * deleted so the platform can confirm the requested action against host
 * reality (the host may delete LESS than requested if the live stack size
 * fell below {@code count} between command issue and handler execution).
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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

    /**
     * L2 object-id of the item instance that was deleted (or decremented).
     */
    public Long getItemId() {
        return itemId;
    }

    /**
     * Number of items actually removed from the stack. MAY be less than the
     * inbound {@link DeleteItemCommand#getCount()} when the live stack size
     * was smaller at execution time (the handler clamps to available).
     */
    public Long getCountDeleted() {
        return countDeleted;
    }

    /**
     * {@code true} when the entire stack was destroyed and the item instance
     * is gone; {@code false} when the stack was decremented (the item
     * instance still exists with a smaller count).
     */
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
