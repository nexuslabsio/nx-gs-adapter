package app.l2nx.gs.adapter.api.kafka.ops;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Heartbeat slot reporting health of the built-in {@code events} module —
 * the bounded-queue + daemon-thread fan-out used for outbound discrete-fact
 * events ({@code events.premium}, future {@code events.character} / etc.).
 *
 * <p>Lives inside {@link ModuleStatus.Stats} alongside {@code pool} (for
 * DB-reading sync modules) and {@code entities} (for per-entity sync
 * progress). Producer side: {@code nx-gs-adapter-core}'s
 * {@code EventsPublisher.currentStatus()}.</p>
 */
public final class EventsStats {

    private final int queueDepth;
    private final int queueCapacity;
    private final long publishedTotal;
    private final long droppedTotal;
    private final long failedTotal;
    private final @Nullable List<String> disabledFamilies;

    public EventsStats(int queueDepth,
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

    /**
     * Current depth of the bounded publish queue at snapshot time.
     */
    public int getQueueDepth() {
        return queueDepth;
    }

    /**
     * Configured queue capacity ({@code l2nx.events.queue-capacity}).
     */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * Total events successfully ack'd by the broker since adapter start.
     */
    public long getPublishedTotal() {
        return publishedTotal;
    }

    /**
     * Total events dropped — sum of queue-overflow drops and shutdown drops
     * since adapter start.
     */
    public long getDroppedTotal() {
        return droppedTotal;
    }

    /**
     * Total events that reached Kafka but the broker callback returned an
     * error (network, NotLeaderForPartition, etc.).
     */
    public long getFailedTotal() {
        return failedTotal;
    }

    /**
     * Event families with no topic configured in
     * {@code MessagingTopics.events} — every {@code NxEvents.publishX(...)}
     * for these families is a no-op. Empty when all known families are wired.
     */
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
        return Objects.hash(queueDepth, queueCapacity, publishedTotal, droppedTotal,
                failedTotal, disabledFamilies);
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
            return new EventsStats(queueDepth, queueCapacity, publishedTotal,
                    droppedTotal, failedTotal, disabledFamilies);
        }
    }
}
