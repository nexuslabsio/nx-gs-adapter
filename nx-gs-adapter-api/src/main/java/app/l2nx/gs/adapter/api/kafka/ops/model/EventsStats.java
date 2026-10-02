package app.l2nx.gs.adapter.api.kafka.ops.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** Heartbeat slot for the built-in {@code events} module (bounded queue + daemon publisher). */
public final class EventsStats {

    private final int queueDepth;
    private final int queueCapacity;
    private final long publishedTotal;
    private final long droppedTotal;
    private final long failedTotal;
    private final @Nullable List<String> disabledFamilies;

    public EventsStats(
            int queueDepth,
            int queueCapacity,
            long publishedTotal,
            long droppedTotal,
            long failedTotal,
            @Nullable List<String> disabledFamilies) {
        this.queueDepth = queueDepth;
        this.queueCapacity = queueCapacity;
        this.publishedTotal = publishedTotal;
        this.droppedTotal = droppedTotal;
        this.failedTotal = failedTotal;
        this.disabledFamilies = freeze(disabledFamilies);
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public long getPublishedTotal() {
        return publishedTotal;
    }

    /** Queue-overflow drops plus shutdown drops. */
    public long getDroppedTotal() {
        return droppedTotal;
    }

    /** Events the broker callback reported as failed. */
    public long getFailedTotal() {
        return failedTotal;
    }

    /** Families with no topic in {@code MessagingTopics.events}; publishing them is a no-op. */
    public List<String> getDisabledFamilies() {
        return disabledFamilies == null ? Collections.emptyList() : disabledFamilies;
    }

    public Builder toBuilder() {
        return new Builder()
                .queueDepth(queueDepth)
                .queueCapacity(queueCapacity)
                .publishedTotal(publishedTotal)
                .droppedTotal(droppedTotal)
                .failedTotal(failedTotal)
                .disabledFamilies(disabledFamilies);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static @Nullable List<String> freeze(@Nullable List<String> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableList(new ArrayList<String>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventsStats)) return false;
        EventsStats that = (EventsStats) o;
        return queueDepth == that.queueDepth
                && queueCapacity == that.queueCapacity
                && publishedTotal == that.publishedTotal
                && droppedTotal == that.droppedTotal
                && failedTotal == that.failedTotal
                && Objects.equals(disabledFamilies, that.disabledFamilies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueDepth, queueCapacity, publishedTotal, droppedTotal, failedTotal, disabledFamilies);
    }

    @Override
    public String toString() {
        return "EventsStats[queueDepth=" + queueDepth
                + ", queueCapacity=" + queueCapacity
                + ", publishedTotal=" + publishedTotal
                + ", droppedTotal=" + droppedTotal
                + ", failedTotal=" + failedTotal
                + ", disabledFamilies=" + disabledFamilies + "]";
    }

    public static final class Builder {
        private int queueDepth;
        private int queueCapacity;
        private long publishedTotal;
        private long droppedTotal;
        private long failedTotal;
        private @Nullable List<String> disabledFamilies;

        public Builder queueDepth(int queueDepth) {
            this.queueDepth = queueDepth;
            return this;
        }

        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder publishedTotal(long publishedTotal) {
            this.publishedTotal = publishedTotal;
            return this;
        }

        public Builder droppedTotal(long droppedTotal) {
            this.droppedTotal = droppedTotal;
            return this;
        }

        public Builder failedTotal(long failedTotal) {
            this.failedTotal = failedTotal;
            return this;
        }

        public Builder disabledFamilies(@Nullable List<String> disabledFamilies) {
            this.disabledFamilies = disabledFamilies;
            return this;
        }

        public EventsStats build() {
            return new EventsStats(
                    queueDepth, queueCapacity, publishedTotal, droppedTotal, failedTotal, disabledFamilies);
        }
    }
}
