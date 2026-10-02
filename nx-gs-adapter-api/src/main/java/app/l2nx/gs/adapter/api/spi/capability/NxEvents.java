package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.spi.ConnectContext;
import org.jspecify.annotations.Nullable;

/**
 * Fan-out of discrete in-game facts to per-family Kafka topics, obtained via {@link ConnectContext#events()};
 * implemented by adapter-core.
 *
 * <p>{@code publish} never blocks beyond a bounded-queue enqueue and never throws; failures show only in
 * heartbeat counters. The event's concrete runtime type must be registered (subclasses do not match), else it
 * is dropped with WARN. Before {@code onConnect}, or for a family without a topic, it is a DEBUG no-op; null is a WARN no-op.</p>
 *
 * <p>Delivery is at-least-once (consumers dedup on the UUIDv7 {@code eventId}); ordering is per partition key.</p>
 */
public interface NxEvents {

    /** Routes by the event's runtime type. */
    void publish(@Nullable Object event);

    /**
     * Blocks until buffered records reach the broker or {@code timeoutMs} elapses; call {@link #publish} first.
     * For JVM-exit paths only (e.g. the server-stopping event), never the game thread. Never throws;
     * a no-op returning true before the producer is wired.
     *
     * @param timeoutMs {@code <= 0} attempts a best-effort drain with no extra wait
     * @return false if the timeout elapsed with records still in flight
     */
    boolean flush(long timeoutMs);
}
