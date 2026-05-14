package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.ops.EventsStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded-queue + single-daemon-thread fan-out for outbound events. Caller
 * threads ({@code NxEventsImpl.publishX(...)}) call {@link #enqueue(EventEnvelope)}
 * which is O(1) and never blocks on Kafka latency. The daemon
 * {@code nx-events-publisher} drains the queue, builds a
 * {@link ProducerRecord}, stamps the {@code Nx-Message-Type} header, and
 * hands off to the supplied {@link Sender} (production: {@code NxKafka.sendBytesKeyRecord}).
 *
 * <p>Drop policy on full queue:</p>
 * <ul>
 *     <li>{@link DropPolicy#NEWEST} (default) — drops the incoming envelope
 *     atomically via {@code queue.offer()} returning {@code false}; no
 *     eviction race under multi-threaded producers.</li>
 *     <li>{@link DropPolicy#OLDEST} — evicts the head and admits the new
 *     envelope so recent facts displace stale snapshots. Head-poll and
 *     newcomer-offer are not atomic, so concurrent producers on a full queue
 *     may over-count {@code droppedTotal} (an enqueue can both evict a head
 *     and lose its slot to another caller).</li>
 * </ul>
 *
 * <p>Counters expose the publisher's health via {@link #currentStatus()}
 * for the heartbeat {@code events} module slot.</p>
 */
public final class EventsPublisher {

    private static final NxLog log = NxLogFactory.getLogger(EventsPublisher.class);

    /**
     * Daemon-loop wake-up cadence — bounds shutdown-signal latency.
     */
    private static final long POLL_TIMEOUT_MS = 100L;

    /**
     * Extra grace beyond {@code shutdownDrainMs} to let the daemon observe the
     * interrupt, run its post-loop shutdown drain, and exit before the join
     * times out.
     */
    private static final long SHUTDOWN_GRACE_MS = 1000L;

    public enum DropPolicy {OLDEST, NEWEST}

    /**
     * Bridge to the actual Kafka send. Production wires this to
     * {@code (record, callback) -> NxKafka.instance().sendBytesKeyRecord(record, callback)};
     * tests inject a recording fake.
     */
    @FunctionalInterface
    public interface Sender {
        void send(ProducerRecord<byte[], Object> record, Callback callback);
    }

    private final Map<String, String> familyTopics;
    private final Sender sender;
    private final BlockingQueue<EventEnvelope> queue;
    private final int queueCapacity;
    private final DropPolicy dropPolicy;
    private final long shutdownDrainMs;
    private final EventTypeRegistry registry;
    private final Thread daemon;

    private final AtomicLong publishedTotal = new AtomicLong();
    private final AtomicLong droppedTotal = new AtomicLong();
    private final AtomicLong failedTotal = new AtomicLong();
    private volatile boolean running = false;

    public EventsPublisher(@Nullable Map<String, String> familyTopics,
                           Sender sender,
                           EventsConfig config,
                           EventTypeRegistry registry) {
        int capacity = Math.max(1, config.getQueueCapacity());
        this.familyTopics = familyTopics == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(familyTopics));
        this.sender = sender;
        this.queue = new ArrayBlockingQueue<EventEnvelope>(capacity);
        this.queueCapacity = capacity;
        this.dropPolicy = config.getDropPolicy() != null ? config.getDropPolicy() : DropPolicy.NEWEST;
        this.shutdownDrainMs = Math.max(0L, config.getShutdownDrainMs());
        this.registry = registry;
        this.daemon = new Thread(SafeRunnable.wrap(this::drainLoop, log), "nx-events-publisher");
        this.daemon.setDaemon(true);
    }

    /**
     * Spawn the publisher daemon. Idempotent. Package-private — callers go
     * through {@link EventsBootstrap}.
     */
    void start() {
        if (running) {
            return;
        }
        running = true;
        daemon.start();
    }

    /**
     * Stop the publisher. Signals the daemon, waits up to
     * {@code shutdownDrainMs} for in-flight envelopes to drain, then cancels.
     * Idempotent.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        daemon.interrupt();
        try {
            daemon.join(shutdownDrainMs + SHUTDOWN_GRACE_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Append an envelope to the publish queue. On full queue, applies the
     * configured {@link DropPolicy}; never blocks longer than the
     * {@code ArrayBlockingQueue.offer()} fast path.
     *
     * <p>Drop counter accounts for every lost envelope, including races. With
     * {@link DropPolicy#OLDEST} and concurrent callers, an enqueue may evict
     * the head AND lose its own envelope to another caller filling the freed
     * slot first — both losses are counted ({@code droppedTotal += 2}).</p>
     */
    public void enqueue(@Nullable EventEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        if (queue.offer(envelope)) {
            return;
        }
        if (dropPolicy == DropPolicy.OLDEST) {
            // Try to evict the head and admit the newcomer. The head poll and
            // newcomer offer are not atomic — a concurrent caller may fill the
            // freed slot before our offer lands.
            boolean evictedHead = (queue.poll() != null);
            if (queue.offer(envelope)) {
                // Happy path: head evicted (if any), newcomer admitted.
                if (evictedHead) {
                    droppedTotal.incrementAndGet();
                }
            } else {
                // Race lost: we may or may not have evicted a head, but the
                // newcomer also failed to land. Count every lost envelope.
                droppedTotal.addAndGet(evictedHead ? 2L : 1L);
            }
        } else {
            droppedTotal.incrementAndGet();
        }
    }

    /**
     * Build a heartbeat slot snapshot.
     *
     * <p>State semantics: {@code ACTIVE} when the daemon is running,
     * {@code DISABLED} when it has not been started or has been stopped.
     * A future enhancement may surface {@code DEGRADED} via a rolling
     * failure-ratio window; operators derive degradation from the raw
     * counters today.</p>
     */
    public ModuleStatus currentStatus() {
        EventsStats stats = EventsStats.builder()
                .queueDepth(queue.size())
                .queueCapacity(queueCapacity)
                .publishedTotal(publishedTotal.get())
                .droppedTotal(droppedTotal.get())
                .failedTotal(failedTotal.get())
                .disabledFamilies(disabledFamilies())
                .build();
        return ModuleStatus.builder()
                .name("events")
                .state(running ? "ACTIVE" : "DISABLED")
                .stats(ModuleStatus.Stats.builder().events(stats).build())
                .build();
    }

    /**
     * True when the family key has a non-empty topic configured.
     */
    public boolean isFamilyEnabled(String familyKey) {
        String topic = familyTopics.get(familyKey);
        return topic != null && !topic.isEmpty();
    }

    private List<String> disabledFamilies() {
        List<String> disabled = new ArrayList<String>();
        for (String family : registry.knownFamilies()) {
            if (!isFamilyEnabled(family)) {
                disabled.add(family);
            }
        }
        return disabled;
    }

    private void drainLoop() {
        while (running) {
            EventEnvelope envelope;
            try {
                envelope = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Either shutdown or interrupt — loop will exit on `running` check.
                Thread.currentThread().interrupt();
                continue;
            }
            if (envelope != null) {
                doSend(envelope);
            }
        }
        drainOnShutdown();
    }

    private void drainOnShutdown() {
        long deadline = System.currentTimeMillis() + shutdownDrainMs;
        while (System.currentTimeMillis() < deadline) {
            EventEnvelope envelope = queue.poll();
            if (envelope == null) {
                return; // queue empty, drained successfully
            }
            doSend(envelope);
        }
        // Anything left after the deadline is dropped.
        long remaining = queue.size();
        if (remaining > 0) {
            droppedTotal.addAndGet(remaining);
            queue.clear();
            log.warn("Events publisher dropped {} envelope(s) on shutdown after {}ms drain", remaining, shutdownDrainMs);
        }
    }

    private void doSend(EventEnvelope envelope) {
        String family = envelope.binding.familyKey();
        String topic = familyTopics.get(family);
        if (topic == null || topic.isEmpty()) {
            // Defensive — NxEventsImpl short-circuits before enqueueing for
            // disabled families, so we should never reach this branch on the
            // happy path. Reaching it means someone enqueued via a path that
            // skipped the short-circuit (e.g. internal tests).
            droppedTotal.incrementAndGet();
            log.debug("events.{} disabled — no topic configured; dropping envelope", family);
            return;
        }
        byte[] partitionKey;
        try {
            partitionKey = envelope.binding.partitionKeyExtractor().apply(envelope.payload);
        } catch (ClassCastException cce) {
            // Type-binding mismatch: the registry returned a binding whose
            // partition extractor casts to a different concrete class than the
            // payload. Indicates a registry-construction bug, not a runtime
            // condition. Surface it explicitly rather than letting the generic
            // Throwable handler swallow the class name.
            failedTotal.incrementAndGet();
            log.error("Events type-binding mismatch for {}: {}",
                    envelope.payload.getClass().getName(), cce.getMessage());
            return;
        }
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<byte[], Object>(
                    topic, partitionKey, envelope.payload);
            record.headers().add(NxHeaders.NX_MESSAGE_TYPE, envelope.binding.messageTypeBytes());
            sender.send(record, (metadata, exception) -> {
                if (exception != null) {
                    failedTotal.incrementAndGet();
                    log.warn("Events publish failed for topic {}: {}", topic, exception.getMessage(), exception);
                } else {
                    publishedTotal.incrementAndGet();
                }
            });
        } catch (Throwable t) {
            failedTotal.incrementAndGet();
            log.error("Events publish threw for topic {}: {}", topic, t.getMessage(), t);
        }
    }

    // Package-visible accessors for tests.

    long publishedTotal() {
        return publishedTotal.get();
    }

    long droppedTotal() {
        return droppedTotal.get();
    }

    long failedTotal() {
        return failedTotal.get();
    }

    int queueDepth() {
        return queue.size();
    }
}
