package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.online.OnlineEvent;
import app.l2nx.gs.adapter.api.kafka.events.online.OnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.online.WellKnownOnlineBuckets;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumEvent;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumPurchaseEvent;
import app.l2nx.gs.commons.UUIDv7;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NxEventsImplTest {

    private EventsPublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.stop();
        }
    }

    @Test
    void publishPremium_shouldEnqueueIntoPublisher() throws InterruptedException {
        ConcurrentLinkedQueue<Object> sent = new ConcurrentLinkedQueue<Object>();
        CountDownLatch latch = new CountDownLatch(1);
        EventsPublisher.Sender sender = (record, callback) -> {
            sent.add(record.value());
            callback.onCompletion(null, null);
            latch.countDown();
        };
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("premium", "acme.gs.events.premium"),
                sender, cfg(50, 500L), registry);
        publisher.start();

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(42L)
                .build();

        events.publishPremium(event);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "publishPremium did not reach sender");
        assertEquals(1, sent.size());
        assertEquals(event, sent.peek());
    }

    @Test
    void publishPremium_shouldNoOp_forNullEvent() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("premium", "acme.gs.events.premium"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPremium(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishPremium_shouldDrop_forUnregisteredSubtype() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("premium", "acme.gs.events.premium"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        // Anonymous subtype with no registry binding.
        events.publishPremium(new PremiumEvent() {
        });

        assertEquals(0, publisher.queueDepth());
        // Dropped at the registry-lookup boundary, not the queue boundary —
        // dropped-total tracks queue evictions only.
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishPremium_shouldShortCircuit_whenFamilyTopicMissing() {
        // No topic for "premium" → publishPremium short-circuits BEFORE enqueueing.
        // Verifies that disabled-family publishes don't burn queue capacity or
        // inflate dropped-total (R10 "no-op + DEBUG log" semantics).
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(Collections.emptyMap(),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPremium(PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(42L)
                .build());

        assertEquals(0, publisher.queueDepth(),
                "disabled family must not enqueue an envelope");
        assertEquals(0L, publisher.droppedTotal(),
                "disabled family must not count toward dropped-total");
    }

    @Test
    void publishOnline_shouldEnqueueIntoPublisher() throws InterruptedException {
        ConcurrentLinkedQueue<Object> sentValues = new ConcurrentLinkedQueue<Object>();
        ConcurrentLinkedQueue<byte[]> sentKeys = new ConcurrentLinkedQueue<byte[]>();
        CountDownLatch latch = new CountDownLatch(1);
        EventsPublisher.Sender sender = (record, callback) -> {
            sentValues.add(record.value());
            sentKeys.add(record.key());
            callback.onCompletion(null, null);
            latch.countDown();
        };
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("online", "acme.gs.events.online"),
                sender, cfg(50, 500L), registry);
        publisher.start();

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .buckets(Collections.singletonMap(WellKnownOnlineBuckets.TOTAL, 1808L))
                .build();

        events.publishOnline(event);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "publishOnline did not reach sender");
        assertEquals(1, sentValues.size());
        assertEquals(event, sentValues.peek());
        assertNull(sentKeys.peek(), "online snapshot partition key must be null (round-robin)");
    }

    @Test
    void publishOnline_shouldNoOp_forNullEvent() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("online", "acme.gs.events.online"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishOnline(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishOnline_shouldDrop_forUnregisteredSubtype() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("online", "acme.gs.events.online"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        // Anonymous subtype with no registry binding.
        events.publishOnline(new OnlineEvent() {
        });

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishOnline_shouldShortCircuit_whenFamilyTopicMissing() {
        // No topic for "online" → publishOnline short-circuits BEFORE enqueueing.
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(Collections.emptyMap(),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishOnline(OnlineSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .buckets(Collections.singletonMap(WellKnownOnlineBuckets.TOTAL, 1L))
                .build());

        assertEquals(0, publisher.queueDepth(),
                "disabled family must not enqueue an envelope");
        assertEquals(0L, publisher.droppedTotal(),
                "disabled family must not count toward dropped-total");
    }

    private static EventsConfig cfg(int capacity, long shutdownDrainMs) {
        return new EventsConfig(capacity, EventsPublisher.DropPolicy.OLDEST, shutdownDrainMs);
    }
}
