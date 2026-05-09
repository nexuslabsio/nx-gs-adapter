package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.events.online.OnlineEvent;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreEvent;

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
     * Publish an event in the {@code premiumpurchase} family.
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

    /**
     * Publish an event in the {@code serveronline} family.
     *
     * <p>{@link OnlineEvent} is the family's abstract base; the concrete
     * subtype (Phase 1: {@code OnlineSnapshotEvent}) is reflected on the
     * platform side via the {@code Nx-Message-Type} Kafka header — adapter-core
     * stamps this header automatically.</p>
     *
     * <p>Cadence is host-managed: the host runs its own scheduler, computes
     * a population breakdown (e.g. by walking its in-memory player set),
     * builds an {@code OnlineSnapshotEvent} and calls this method. The
     * adapter neither dictates the interval nor pulls — it only provides
     * the wire path. Typical cadence is 30–60 seconds.</p>
     *
     * <p>Returns immediately after enqueueing. Same delivery semantics as
     * {@link #publishPremium} — at-least-once, idempotency on UUIDv7
     * {@code eventId}.</p>
     *
     * @param event non-null online event; {@code null} is treated as a no-op
     *              with a WARN log entry (does not throw — game-loop safety).
     */
    void publishOnline(OnlineEvent event);

    /**
     * Publish an event in the {@code privatestore} family.
     *
     * <p>{@link PrivateStoreEvent} is the family's abstract base; concrete
     * subtypes ({@code PrivateStoreTradeEvent} /
     * {@code PrivateStoreSnapshotEvent}) are reflected on the platform side
     * via the {@code Nx-Message-Type} Kafka header (carrying the simple
     * class name) — adapter-core stamps this header automatically.</p>
     *
     * <p><b>Two production patterns share this entry-point.</b> Trade events
     * are pushed by host hooks at the moment a private-store deal is
     * finalized on the game thread (one event per closed transaction,
     * possibly multi-line). Snapshot events are pushed by a host-managed
     * daemon on a configured cadence — one event per
     * {@code (itemId, side)} pair whose order book changed since the
     * previous tick, plus one tombstone event ({@code offers=[]}) when a
     * tracked pair empties. Change-detection is the host's responsibility;
     * the adapter only provides the wire path.</p>
     *
     * <p>Returns immediately after enqueueing. Same delivery semantics as
     * {@link #publishPremium} — at-least-once, idempotency on UUIDv7
     * {@code eventId}.</p>
     *
     * <p>Trade events are partitioned round-robin (no single natural
     * per-entity key — buyer and seller are equally valid). Snapshot events
     * are partitioned by {@code itemId} so all updates for the same item
     * land on one partition for ordered consumption / topic-compaction-friendly
     * "latest known book" caching.</p>
     *
     * @param event non-null private-store event; {@code null} is treated as a
     *              no-op with a WARN log entry (does not throw —
     *              game-loop safety).
     */
    void publishPrivateStore(PrivateStoreEvent event);
}
