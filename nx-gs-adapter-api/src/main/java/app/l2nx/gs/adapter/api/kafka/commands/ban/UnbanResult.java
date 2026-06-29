package app.l2nx.gs.adapter.api.kafka.commands.ban;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Success payload of {@link UnbanCommand}. Reports whether the requested ban was
 * present and the ids of the ban rows the host cleared.
 *
 * <p>{@link #isRemoved() removed} is {@code false} when no matching ban existed
 * (a no-op success). {@link #getRemovedBanIds() removedBanIds}
 * lists the rows that were deleted — empty when nothing matched or when the ban
 * kind is not persisted as an id-bearing row.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class UnbanResult {

    private final boolean removed;
    private final List<Long> removedBanIds;

    public UnbanResult(boolean removed, List<Long> removedBanIds) {
        this.removed = removed;
        this.removedBanIds =
                removedBanIds == null ? Collections.<Long>emptyList() : Collections.unmodifiableList(removedBanIds);
    }

    /**
     * Whether a matching ban existed and was cleared. {@code false} is a no-op
     * success — the post-condition (no such ban) already held.
     */
    public boolean isRemoved() {
        return removed;
    }

    /**
     * Ids of the ban rows cleared by this unban. Never {@code null};
     * empty when nothing matched or the ban kind is not an id-bearing row.
     */
    public List<Long> getRemovedBanIds() {
        return removedBanIds;
    }

    public Builder toBuilder() {
        return new Builder().removed(removed).removedBanIds(removedBanIds);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UnbanResult)) return false;
        UnbanResult that = (UnbanResult) o;
        return removed == that.removed && removedBanIds.equals(that.removedBanIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(removed, removedBanIds);
    }

    @Override
    public String toString() {
        return "UnbanResult[removed=" + removed + ", removedBanIds=" + removedBanIds + "]";
    }

    public static final class Builder {
        private boolean removed;
        private List<Long> removedBanIds;

        public Builder removed(boolean removed) {
            this.removed = removed;
            return this;
        }

        public Builder removedBanIds(List<Long> removedBanIds) {
            this.removedBanIds = removedBanIds;
            return this;
        }

        public UnbanResult build() {
            return new UnbanResult(removed, removedBanIds);
        }
    }
}
