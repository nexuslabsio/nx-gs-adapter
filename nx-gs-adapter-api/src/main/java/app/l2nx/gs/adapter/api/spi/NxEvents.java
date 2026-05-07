package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumEvent;

/**
 * Adapter-side capability for fanning out discrete in-game facts to the
 * platform via per-family Kafka topics. Acquired via
 * {@link ConnectContext#events()}; the implementation is built into
 * {@code nx-gs-adapter-core} and is NOT a {@code ServiceLoader}-discovered
 * SPI — tenants consume this interface, they do not implement it.
 *
 * <p>One method per event family. Adding a family is a binary-compatible
 * API expansion — host code calls these methods, doesn't implement the
 * interface. Within a family, growth is free: a new
 * {@code PremiumRefundEvent extends PremiumEvent} subtype reuses the same
 * {@link #publishPremium(PremiumEvent)} entry-point, dispatched on the
 * platform consumer side via the {@code Nx-Message-Type} Kafka header.</p>
 *
 * <p><b>Game-loop safety contract.</b> Implementations MUST NOT block the
 * caller longer than enqueueing a record into a bounded queue, MUST NOT
 * throw, and MUST NOT propagate any internal failure (Kafka producer error,
 * serialization fault) up the call chain. Failure is observable through
 * the heartbeat counters surfaced as the {@code events} module slot.</p>
 *
 * <p><b>Pre-connect calls.</b> Calling a publish method before
 * {@code onConnect} completes (i.e. before the Kafka producer is wired) is
 * a no-op with a DEBUG log entry. Phase-1 host hooks fire post-{@code start()}
 * so this is rarely hit; pre-connect buffering is reserved for a future
 * opt-in feature.</p>
 *
 * <p><b>Topic resolution.</b> Each family's topic is resolved from
 * {@code MessagingTopics.events} on the {@link ConnectContext}'s
 * {@code ConnectResponse}. A family without a configured topic is
 * disabled — its publish method becomes a no-op with a DEBUG log entry.
 * Operators see disabled families on the heartbeat
 * {@code events.disabled-families} slot.</p>
 */
public interface NxEvents {

    /**
     * Publish an event in the {@code premium} family.
     *
     * <p>{@link PremiumEvent} is the family's abstract base; the concrete
     * subtype (e.g. {@code PremiumPurchaseEvent}) is reflected on the
     * platform side via the {@code Nx-Message-Type} Kafka header (carrying
     * the simple class name) — adapter-core stamps this header automatically.</p>
     *
     * <p>Returns immediately after enqueueing. Caller MUST NOT assume
     * delivery — at-least-once semantics, idempotency by {@code eventId}
     * (UUIDv7) on the consumer side.</p>
     *
     * @param event non-null premium event; {@code null} is treated as a no-op
     *              with a WARN log entry (does not throw — game-loop safety).
     */
    void publishPremium(PremiumEvent event);
}
