package app.l2nx.gs.adapter.core.events;

/**
 * Operator-tunable knobs for the built-in events publisher. Resolved by
 * {@code ConfigResolver.resolveEventsConfig()} via the file-first source
 * chain ({@code l2nx.properties} → JVM system property → built-in default).
 *
 * <p>Drop policy default is {@link EventsPublisher.DropPolicy#NEWEST}.
 * {@link EventsPublisher.DropPolicy#OLDEST} is still supported but exhibits
 * eviction contention under multi-threaded producers — head-poll and newcomer-
 * offer are not atomic, so a hot path can over-count {@code droppedTotal} when
 * threads race on a full queue. NEWEST drops the incoming envelope atomically
 * via {@code queue.offer()} returning {@code false}, no eviction race.</p>
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
