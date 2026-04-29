package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.Objects;

/**
 * Per-entity change counts for the last completed CDC cycle. Surfaced inside
 * {@link EntityStats#getLastCycleChanges()}.
 */
public final class ChangesSummary {

    private final long created;
    private final long updated;
    private final long deleted;

    public ChangesSummary(long created, long updated, long deleted) {
        this.created = created;
        this.updated = updated;
        this.deleted = deleted;
    }

    public long getCreated() {
        return created;
    }

    public long getUpdated() {
        return updated;
    }

    public long getDeleted() {
        return deleted;
    }

    public Builder toBuilder() {
        return new Builder().created(created).updated(updated).deleted(deleted);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChangesSummary)) return false;
        ChangesSummary that = (ChangesSummary) o;
        return created == that.created && updated == that.updated && deleted == that.deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, updated, deleted);
    }

    @Override
    public String toString() {
        return "ChangesSummary[created=" + created + ", updated=" + updated + ", deleted=" + deleted + "]";
    }

    public static final class Builder {
        private long created;
        private long updated;
        private long deleted;

        public Builder created(long created) {
            this.created = created;
            return this;
        }

        public Builder updated(long updated) {
            this.updated = updated;
            return this;
        }

        public Builder deleted(long deleted) {
            this.deleted = deleted;
            return this;
        }

        public ChangesSummary build() {
            return new ChangesSummary(created, updated, deleted);
        }
    }
}
