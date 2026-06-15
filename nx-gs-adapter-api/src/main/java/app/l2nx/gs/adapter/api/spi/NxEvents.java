package app.l2nx.gs.adapter.api.spi;

import org.jspecify.annotations.Nullable;

/**
 * Adapter-side capability for fanning out discrete in-game facts to the
 * platform via per-family Kafka topics. Acquired via
 * {@link ConnectContext#events()}; the implementation is built into
 * {@code nx-gs-adapter-core} and is NOT a {@code ServiceLoader}-discovered
 * SPI — tenants consume this interface, they do not implement it.
 *
 * <p>Single generic publish method — runtime type of the {@code event}
 * argument routes to the registered family (topic + partition-key extractor +
 * {@code Nx-Message-Type} header value). Per-family wire semantics
 * (topic name, partition key, ordering guarantees) live on the event DTOs
 * themselves and in the adapter-core type registry; the interface stays one
 * method regardless of how many families ship.</p>
 *
 * <p><b>Routing rules.</b> The runtime type of {@code event} MUST match a
 * concrete event class registered in {@code EventTypeRegistry}
 * (e.g. {@code RaidKillEvent}, {@code CharacterPresenceEvent},
 * {@code PrivateStoreSnapshotEvent}). Subclasses are NOT matched — pass the
 * concrete type. An unregistered type drops with a WARN log entry.</p>
 *
 * <p><b>Game-loop safety contract.</b> Implementations MUST NOT block the
 * caller longer than enqueueing a record into a bounded queue, MUST NOT
 * throw, and MUST NOT propagate any internal failure (Kafka producer error,
 * serialization fault) up the call chain. Failure is observable through
 * the heartbeat counters surfaced as the {@code events} module slot.</p>
 *
 * <p><b>Delivery semantics.</b> At-least-once with consumer-side dedup on
 * the event's UUIDv7 {@code eventId}. Partition key is per-family (see the
 * concrete DTO's Javadoc); ordering guarantees are partition-scoped.</p>
 *
 * <p><b>Pre-connect calls.</b> Calling {@link #publish} before
 * {@code onConnect} completes (i.e. before the Kafka producer is wired) is
 * a no-op with a DEBUG log entry. Pre-connect buffering is reserved for a
 * future opt-in feature.</p>
 *
 * <p><b>Family disabled.</b> Each family's topic is resolved from
 * {@code MessagingTopics.events} on the {@link ConnectContext}'s
 * {@code ConnectResponse}. A family without a configured topic is disabled
 * — {@link #publish} becomes a no-op with a DEBUG log entry for that family.
 * Operators see disabled families on the heartbeat
 * {@code events.disabled-families} slot.</p>
 *
 * <p><b>Null event.</b> A {@code null} argument is a no-op with a WARN log
 * entry — never throws (game-loop safety).</p>
 */
public interface NxEvents {

    /**
     * Publish an event. The runtime type of {@code event} resolves the wire
     * family (topic + partition-key extractor + message-type header) via the
     * adapter-core type registry. See the interface-level Javadoc for routing
     * rules, delivery semantics, and failure handling.
     *
     * @param event concrete event DTO from {@code kafka.events.<family>.*};
     *              {@code null} is treated as a no-op with a WARN log entry.
     */
    void publish(@Nullable Object event);

    /**
     * Synchronously drain the in-memory event queue into the Kafka producer and
     * block until every buffered record has been sent to the broker, or until
     * {@code timeoutMs} elapses — whichever comes first. The single sync method
     * on this otherwise fire-and-forget contract.
     *
     * <p>Unlike {@link #publish}, this BLOCKS the caller. Use it on a JVM-exit
     * path (e.g. just before {@code System.exit}) to guarantee a freshly
     * published fact — typically the server-stopping event — reaches the broker
     * before shutdown hooks tear the producer down. Callers MUST {@link #publish}
     * the event first, then {@code flush}.</p>
     *
     * <p>Unlike {@code publish}'s game-loop-safety contract, this method MAY
     * block for up to {@code timeoutMs}; never call it on the game thread.
     * It never throws — failures and timeouts are swallowed (and surface only
     * through the return value / heartbeat counters).</p>
     *
     * <p><b>Pre-connect.</b> Before {@code onConnect} wires the producer this is
     * a no-op returning {@code true} (nothing to flush), mirroring {@code publish}.</p>
     *
     * @param timeoutMs maximum time to block, in milliseconds; {@code <= 0}
     *                  attempts a best-effort drain + flush with no extra wait
     * @return {@code true} if the queue drained and the producer flushed within
     * the budget (or there was nothing to flush); {@code false} if the
     * timeout elapsed with records still in flight
     */
    boolean flush(long timeoutMs);
}
