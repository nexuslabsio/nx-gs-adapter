package app.l2nx.gs.adapter.core.events;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.ops.EventsStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.commons.UUIDv7;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventsPublisherTest {

    private EventsPublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.stop();
        }
    }

    @Test
    void enqueue_shouldDispatchToSender_throughDaemonThread() throws InterruptedException {
        ConcurrentLinkedQueue<ProducerRecord<byte[], Object>> sent =
                new ConcurrentLinkedQueue<ProducerRecord<byte[], Object>>();
        CountDownLatch latch = new CountDownLatch(1);
        EventsPublisher.Sender sender = (record, callback) -> {
            sent.add(record);
            callback.onCompletion(null, null);
            latch.countDown();
        };

        Map<String, String> topics = Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase");
        publisher = new EventsPublisher(
                topics, sender, cfg(100, EventsPublisher.DropPolicy.OLDEST, 500L), new EventTypeRegistry());
        publisher.start();

        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(42L)
                .build();
        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);
        publisher.enqueue(new EventEnvelope(event, binding));

        assertTrue(latch.await(2, TimeUnit.SECONDS), "sender not invoked within 2s");
        ProducerRecord<byte[], Object> record = sent.peek();
        assertNotNull(record);
        assertEquals("acme.gs.events.premiumpurchase", record.topic());

        // Partition key is 8 raw BE bytes of characterId.
        long extracted = ByteBuffer.wrap(record.key()).getLong();
        assertEquals(42L, extracted);

        // Nx-Message-Type header carries the simple class name.
        Header header = record.headers().lastHeader(NxHeaders.NX_MESSAGE_TYPE);
        assertNotNull(header);
        assertEquals("PremiumPurchaseEvent", new String(header.value(), java.nio.charset.StandardCharsets.UTF_8));

        // Successful ack increments publishedTotal.
        Thread.sleep(50);
        assertEquals(1L, publisher.publishedTotal());
        assertEquals(0L, publisher.droppedTotal());
        assertEquals(0L, publisher.failedTotal());
    }

    @Test
    void enqueue_shouldDropOldest_whenQueueIsFull() {
        // Use a non-started publisher to inspect raw queue / counter behavior.
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                cfg(2, EventsPublisher.DropPolicy.OLDEST, 0L),
                new EventTypeRegistry());
        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);

        publisher.enqueue(envelope(1L, binding));
        publisher.enqueue(envelope(2L, binding));
        // Queue full — third enqueue evicts the head (envelope 1).
        publisher.enqueue(envelope(3L, binding));

        assertEquals(2, publisher.queueDepth());
        assertEquals(1L, publisher.droppedTotal());
    }

    @Test
    void enqueue_shouldEvictOldestEnvelope_inDropOldestMode_verifyingOrder() throws InterruptedException {
        // Capacity 2, three enqueues ordered 1→2→3. After eviction the daemon
        // should drain envelopes 2 and 3 in that order; envelope 1 is gone.
        ConcurrentLinkedQueue<Long> drained = new ConcurrentLinkedQueue<Long>();
        CountDownLatch latch = new CountDownLatch(2);
        EventsPublisher.Sender sender = (record, callback) -> {
            drained.add(ByteBuffer.wrap(record.key()).getLong());
            callback.onCompletion(null, null);
            latch.countDown();
        };

        Map<String, String> topics = Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase");
        EventTypeRegistry registry = new EventTypeRegistry();
        // shutdownDrainMs=0 keeps tearDown.stop() fast; daemon-poll grace is enough
        // to drain 2 envelopes before assertion.
        publisher = new EventsPublisher(topics, sender, cfg(2, EventsPublisher.DropPolicy.OLDEST, 0L), registry);
        EventTypeBinding binding = registry.lookup(PremiumPurchaseEvent.class);

        // Enqueue all three BEFORE starting the daemon — otherwise the daemon may
        // drain 1 before 3 arrives, leaving 2+3 in queue without an eviction race.
        publisher.enqueue(envelope(1L, binding));
        publisher.enqueue(envelope(2L, binding));
        publisher.enqueue(envelope(3L, binding));
        // Eviction has happened on the caller thread; queue now holds 2, 3.
        publisher.start();

        assertTrue(latch.await(2, TimeUnit.SECONDS), "daemon did not drain in 2s");
        assertEquals(2, drained.size());
        assertEquals(Long.valueOf(2L), drained.poll());
        assertEquals(Long.valueOf(3L), drained.poll());
        assertEquals(1L, publisher.droppedTotal());
    }

    @Test
    void enqueue_shouldDropNewest_whenPolicyIsNewest() {
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                cfg(1, EventsPublisher.DropPolicy.NEWEST, 0L),
                new EventTypeRegistry());
        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);

        publisher.enqueue(envelope(1L, binding));
        publisher.enqueue(envelope(2L, binding));

        assertEquals(1, publisher.queueDepth());
        assertEquals(1L, publisher.droppedTotal());
    }

    @Test
    void enqueue_shouldNoOp_forNullEnvelope() {
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                cfg(5, EventsPublisher.DropPolicy.OLDEST, 0L),
                new EventTypeRegistry());

        publisher.enqueue(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void doSend_shouldDrop_whenFamilyTopicMissing() throws InterruptedException {
        // No topic for "premiumpurchase" → enqueued envelopes drop on the daemon thread.
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                cfg(5, EventsPublisher.DropPolicy.OLDEST, 100L),
                new EventTypeRegistry());
        publisher.start();

        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);
        publisher.enqueue(envelope(99L, binding));

        // Wait briefly for daemon to drain.
        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline && publisher.droppedTotal() == 0) {
            Thread.sleep(20);
        }

        assertEquals(1L, publisher.droppedTotal());
        assertEquals(0L, publisher.publishedTotal());
    }

    @Test
    void currentStatus_shouldReportDisabledFamilies_whenTopicMissing() {
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                cfg(5, EventsPublisher.DropPolicy.OLDEST, 0L),
                new EventTypeRegistry());

        ModuleStatus status = publisher.currentStatus();

        assertEquals("events", status.getName());
        EventsStats stats = status.getStats().getEvents().orElseThrow(() -> new AssertionError("missing events stats"));
        List<String> disabled = stats.getDisabledFamilies();
        assertTrue(
                disabled.contains("premiumpurchase"),
                "expected 'premiumpurchase' in disabled-families, got " + disabled);
    }

    @Test
    void currentStatus_shouldReportNoDisabledFamilies_whenAllConfigured() {
        Map<String, String> topics = new HashMap<String, String>();
        topics.put("premiumpurchase", "acme.gs.events.premiumpurchase");
        topics.put("serveronline", "acme.gs.events.serveronline");
        topics.put("privatestore", "acme.gs.events.privatestore");
        topics.put("character", "acme.gs.events.character");
        topics.put("raid", "acme.gs.events.raid");
        topics.put("mail", "acme.gs.events.mail");
        topics.put("privatetrade", "acme.gs.events.privatetrade");
        topics.put("olympiad", "acme.gs.events.olympiad");
        topics.put("account", "acme.ls.events.account");
        topics.put("gameevents", "acme.gs.events.gameevents");
        topics.put("castle", "acme.gs.events.castle");
        topics.put("sync", "acme.gs.events.sync");
        publisher = new EventsPublisher(
                topics, noopSender(), cfg(5, EventsPublisher.DropPolicy.OLDEST, 0L), new EventTypeRegistry());

        ModuleStatus status = publisher.currentStatus();

        EventsStats stats = status.getStats().getEvents().orElseThrow(() -> new AssertionError("missing events stats"));
        assertTrue(stats.getDisabledFamilies().isEmpty());
    }

    @Test
    void currentStatus_shouldExposeQueueCapacityAndDepth() {
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                cfg(7, EventsPublisher.DropPolicy.OLDEST, 0L),
                new EventTypeRegistry());

        EventsStats stats = publisher
                .currentStatus()
                .getStats()
                .getEvents()
                .orElseThrow(() -> new AssertionError("missing events stats"));
        assertEquals(7, stats.getQueueCapacity());
        assertEquals(0, stats.getQueueDepth());
    }

    @Test
    void flush_shouldDrainQueueAndInvokeProducerFlush() {
        ConcurrentLinkedQueue<Long> sent = new ConcurrentLinkedQueue<Long>();
        EventsPublisher.Sender sender = (record, callback) -> {
            sent.add(ByteBuffer.wrap(record.key()).getLong());
            callback.onCompletion(null, null);
        };
        java.util.concurrent.atomic.AtomicInteger flushes = new java.util.concurrent.atomic.AtomicInteger();
        EventsPublisher.ProducerFlusher flusher = flushes::incrementAndGet;

        Map<String, String> topics = Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase");
        EventTypeRegistry registry = new EventTypeRegistry();
        // Non-started publisher — flush() drains synchronously on the calling thread.
        publisher =
                new EventsPublisher(topics, sender, flusher, cfg(10, EventsPublisher.DropPolicy.NEWEST, 0L), registry);
        EventTypeBinding binding = registry.lookup(PremiumPurchaseEvent.class);
        publisher.enqueue(envelope(1L, binding));
        publisher.enqueue(envelope(2L, binding));

        boolean completed = publisher.flush(1000L);

        assertTrue(completed);
        assertEquals(2, sent.size());
        assertEquals(0, publisher.queueDepth());
        assertEquals(1, flushes.get());
    }

    @Test
    void flush_shouldReturnTrueAndFlush_whenQueueEmpty() {
        java.util.concurrent.atomic.AtomicInteger flushes = new java.util.concurrent.atomic.AtomicInteger();
        publisher = new EventsPublisher(
                Collections.emptyMap(),
                noopSender(),
                flushes::incrementAndGet,
                cfg(10, EventsPublisher.DropPolicy.NEWEST, 0L),
                new EventTypeRegistry());

        assertTrue(publisher.flush(1000L));
        assertEquals(1, flushes.get());
    }

    private static EventsConfig cfg(int capacity, EventsPublisher.DropPolicy policy, long shutdownDrainMs) {
        return new EventsConfig(capacity, policy, shutdownDrainMs);
    }

    private static EventEnvelope envelope(long charId, EventTypeBinding binding) {
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(charId)
                .build();
        return new EventEnvelope(event, binding);
    }

    private static EventsPublisher.Sender noopSender() {
        return (record, callback) -> {
            // never invoke callback — keeps publishedTotal at 0 in queue-only tests
        };
    }
}
