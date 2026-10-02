package app.l2nx.gs.adapter.core.events;

/**
 * OLDEST drop policy is racy under multi-threaded producers (head-poll and newcomer-offer are not atomic),
 * so it can over-count {@code droppedTotal}; NEWEST drops atomically.
 */
public final class EventsConfig {

    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    public static final EventsPublisher.DropPolicy DEFAULT_DROP_POLICY = EventsPublisher.DropPolicy.NEWEST;
    public static final long DEFAULT_SHUTDOWN_DRAIN_MS = 5_000L;

    private final int queueCapacity;
    private final EventsPublisher.DropPolicy dropPolicy;
    private final long shutdownDrainMs;

    public EventsConfig(int queueCapacity, EventsPublisher.DropPolicy dropPolicy, long shutdownDrainMs) {
        this.queueCapacity = queueCapacity;
        this.dropPolicy = dropPolicy;
        this.shutdownDrainMs = shutdownDrainMs;
    }

    public static EventsConfig defaults() {
        return new EventsConfig(DEFAULT_QUEUE_CAPACITY, DEFAULT_DROP_POLICY, DEFAULT_SHUTDOWN_DRAIN_MS);
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public EventsPublisher.DropPolicy getDropPolicy() {
        return dropPolicy;
    }

    public long getShutdownDrainMs() {
        return shutdownDrainMs;
    }
}
