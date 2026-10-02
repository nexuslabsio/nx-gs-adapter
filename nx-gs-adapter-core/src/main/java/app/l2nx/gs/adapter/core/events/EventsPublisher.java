package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.ops.model.EventsStats;
import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStatus;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;

/**
 * Bounded queue drained by one daemon thread so callers never block on Kafka latency.
 * With OLDEST, head-poll and newcomer-offer are not atomic, so concurrent producers may over-count droppedTotal.
 */
public final class EventsPublisher {

    private static final NxLog log = NxLogFactory.getLogger(EventsPublisher.class);

    /** Bounds shutdown-signal latency. */
    private static final long POLL_TIMEOUT_MS = 100L;

    /** Lets the daemon finish its post-loop drain before the join times out. */
    private static final long SHUTDOWN_GRACE_MS = 1000L;

    private static final ProducerFlusher NO_OP_FLUSHER = () -> {};

    public enum DropPolicy {
        OLDEST,
        NEWEST
    }

    @FunctionalInterface
    public interface Sender {
        void send(ProducerRecord<byte[], Object> record, Callback callback);
    }

    /** Blocks until in-flight sends reach the broker. */
    @FunctionalInterface
    public interface ProducerFlusher {
        void flush();
    }

    private final Map<String, String> familyTopics;
    private final Sender sender;
    private final ProducerFlusher producerFlusher;
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

    public EventsPublisher(
            @Nullable Map<String, String> familyTopics,
            Sender sender,
            EventsConfig config,
            EventTypeRegistry registry) {
        this(familyTopics, sender, NO_OP_FLUSHER, config, registry);
    }

    public EventsPublisher(
            @Nullable Map<String, String> familyTopics,
            Sender sender,
            ProducerFlusher producerFlusher,
            EventsConfig config,
            EventTypeRegistry registry) {
        int capacity = Math.max(1, config.getQueueCapacity());
        this.familyTopics = familyTopics == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(familyTopics));
        this.sender = sender;
        this.producerFlusher = producerFlusher != null ? producerFlusher : NO_OP_FLUSHER;
        this.queue = new ArrayBlockingQueue<EventEnvelope>(capacity);
        this.queueCapacity = capacity;
        this.dropPolicy = config.getDropPolicy() != null ? config.getDropPolicy() : DropPolicy.NEWEST;
        this.shutdownDrainMs = Math.max(0L, config.getShutdownDrainMs());
        this.registry = registry;
        this.daemon = new Thread(SafeRunnable.wrap(this::drainLoop, log), "nx-events-publisher");
        this.daemon.setDaemon(true);
    }

    void start() {
        if (running) {
            return;
        }
        running = true;
        daemon.start();
    }

    /** Waits up to {@code shutdownDrainMs} for in-flight envelopes, then cancels. */
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

    /** With OLDEST, an enqueue may evict the head and lose its own envelope to a racing caller; both losses are counted. */
    public void enqueue(@Nullable EventEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        if (queue.offer(envelope)) {
            return;
        }
        if (dropPolicy == DropPolicy.OLDEST) {
            // head poll and newcomer offer are not atomic
            boolean evictedHead = (queue.poll() != null);
            if (queue.offer(envelope)) {
                if (evictedHead) {
                    droppedTotal.incrementAndGet();
                }
            } else {
                // newcomer lost its slot to a racing caller; count every lost envelope
                droppedTotal.addAndGet(evictedHead ? 2L : 1L);
            }
        } else {
            droppedTotal.incrementAndGet();
        }
    }

    /**
     * Does NOT stop the daemon; both feed the same thread-safe sender.
     * Never throws (game-exit safety); returns {@code false} on timeout or flusher failure.
     */
    public boolean flush(long timeoutMs) {
        long deadline = System.currentTimeMillis() + Math.max(0L, timeoutMs);
        EventEnvelope envelope;
        while ((envelope = queue.poll()) != null) {
            doSend(envelope);
            if (timeoutMs > 0 && System.currentTimeMillis() >= deadline) {
                log.warn(
                        "Events flush timed out draining queue after {}ms — {} envelope(s) still queued",
                        timeoutMs,
                        queue.size());
                return false;
            }
        }
        try {
            producerFlusher.flush();
        } catch (Throwable t) {
            log.warn("Events flush: producer flush threw {} — {}", t.getClass().getName(), t.getMessage());
            return false;
        }
        return timeoutMs <= 0 || System.currentTimeMillis() < deadline;
    }

    /** {@code DEGRADED} is never surfaced; operators derive degradation from the raw counters. */
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
                Thread.currentThread().interrupt();
                continue;
            }
            if (envelope != null) {
                doSend(envelope);
            }
        }
        drainOnShutdown();
    }

    // package-visible for unit tests
    void drainOnShutdown() {
        long deadline = System.currentTimeMillis() + shutdownDrainMs;
        while (System.currentTimeMillis() < deadline) {
            EventEnvelope envelope = queue.poll();
            if (envelope == null) {
                return;
            }
            doSend(envelope);
        }
        long remaining = queue.size();
        if (remaining > 0) {
            droppedTotal.addAndGet(remaining);
            queue.clear();
            log.warn(
                    "Events publisher dropped {} envelope(s) on shutdown after {}ms drain", remaining, shutdownDrainMs);
        }
    }

    private void doSend(EventEnvelope envelope) {
        String family = envelope.binding.familyKey();
        String topic = familyTopics.get(family);
        if (topic == null || topic.isEmpty()) {
            // NxEventsImpl short-circuits disabled families; reaching this means a path skipped it
            droppedTotal.incrementAndGet();
            log.debug("events.{} disabled — no topic configured; dropping envelope", family);
            return;
        }
        byte[] partitionKey;
        try {
            partitionKey = envelope.binding.partitionKeyExtractor().apply(envelope.payload);
        } catch (ClassCastException cce) {
            // registry-construction bug; log explicitly instead of letting the generic handler swallow the class name
            failedTotal.incrementAndGet();
            log.error(
                    "Events type-binding mismatch for {}: {}",
                    envelope.payload.getClass().getName(),
                    cce.getMessage());
            return;
        }
        try {
            ProducerRecord<byte[], Object> record =
                    new ProducerRecord<byte[], Object>(topic, partitionKey, envelope.payload);
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
