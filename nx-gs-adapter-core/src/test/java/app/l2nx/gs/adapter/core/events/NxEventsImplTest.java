package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSide;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.WellKnownServerOnlineBuckets;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.commons.bytes.LongBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    void publishPremiumPurchase_shouldEnqueueIntoPublisher() throws InterruptedException {
        ConcurrentLinkedQueue<Object> sent = new ConcurrentLinkedQueue<Object>();
        CountDownLatch latch = new CountDownLatch(1);
        EventsPublisher.Sender sender = (record, callback) -> {
            sent.add(record.value());
            callback.onCompletion(null, null);
            latch.countDown();
        };
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase"),
                sender, cfg(50, 500L), registry);
        publisher.start();

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(42L)
                .build();

        events.publishPremiumPurchase(event);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "publishPremiumPurchase did not reach sender");
        assertEquals(1, sent.size());
        assertEquals(event, sent.peek());
    }

    @Test
    void publishPremiumPurchase_shouldNoOp_forNullEvent() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("premiumpurchase", "acme.gs.events.premiumpurchase"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPremiumPurchase(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishPremiumPurchase_shouldShortCircuit_whenFamilyTopicMissing() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(Collections.emptyMap(),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPremiumPurchase(PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(42L)
                .build());

        assertEquals(0, publisher.queueDepth(),
                "disabled family must not enqueue an envelope");
        assertEquals(0L, publisher.droppedTotal(),
                "disabled family must not count toward dropped-total");
    }

    @Test
    void publishServerOnlineSnapshot_shouldEnqueueIntoPublisher() throws InterruptedException {
        ConcurrentLinkedQueue<Object> sentValues = new ConcurrentLinkedQueue<Object>();
        // ConcurrentLinkedQueue rejects nulls, so partition-key=null observation
        // is recorded via a flag rather than queueing the byte[].
        AtomicBoolean partitionKeyWasNull = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        EventsPublisher.Sender sender = (record, callback) -> {
            sentValues.add(record.value());
            partitionKeyWasNull.set(record.key() == null);
            callback.onCompletion(null, null);
            latch.countDown();
        };
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("serveronline", "acme.gs.events.serveronline"),
                sender, cfg(50, 500L), registry);
        publisher.start();

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        ServerOnlineSnapshotEvent event = ServerOnlineSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .buckets(Collections.singletonMap(WellKnownServerOnlineBuckets.TOTAL, 1808L))
                .build();

        events.publishServerOnlineSnapshot(event);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "publishServerOnlineSnapshot did not reach sender");
        assertEquals(1, sentValues.size());
        assertEquals(event, sentValues.peek());
        assertTrue(partitionKeyWasNull.get(),
                "server-online snapshot partition key must be null (round-robin)");
    }

    @Test
    void publishServerOnlineSnapshot_shouldNoOp_forNullEvent() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("serveronline", "acme.gs.events.serveronline"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishServerOnlineSnapshot(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishServerOnlineSnapshot_shouldShortCircuit_whenFamilyTopicMissing() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(Collections.emptyMap(),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishServerOnlineSnapshot(ServerOnlineSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .buckets(Collections.singletonMap(WellKnownServerOnlineBuckets.TOTAL, 1L))
                .build());

        assertEquals(0, publisher.queueDepth(),
                "disabled family must not enqueue an envelope");
        assertEquals(0L, publisher.droppedTotal(),
                "disabled family must not count toward dropped-total");
    }

    @Test
    void publishPrivateStorePurchase_shouldEnqueueRoundRobin() throws InterruptedException {
        ConcurrentLinkedQueue<Object> sentValues = new ConcurrentLinkedQueue<Object>();
        AtomicBoolean partitionKeyWasNull = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        EventsPublisher.Sender sender = (record, callback) -> {
            sentValues.add(record.value());
            partitionKeyWasNull.set(record.key() == null);
            callback.onCompletion(null, null);
            latch.countDown();
        };
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("privatestore", "acme.gs.events.privatestore"),
                sender, cfg(50, 500L), registry);
        publisher.start();

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .build();

        events.publishPrivateStorePurchase(event);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "publishPrivateStorePurchase did not reach sender");
        assertEquals(1, sentValues.size());
        assertEquals(event, sentValues.peek());
        assertTrue(partitionKeyWasNull.get(),
                "private-store purchase partition key must be null (round-robin)");
    }

    @Test
    void publishPrivateStoreSnapshot_shouldEnqueuePartitionedByItemId() throws InterruptedException {
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
                Collections.singletonMap("privatestore", "acme.gs.events.privatestore"),
                sender, cfg(50, 500L), registry);
        publisher.start();

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .itemId(0xCAFEBABEL)
                .side(PrivateStoreSide.ASK)
                .build();

        events.publishPrivateStoreSnapshot(event);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "publishPrivateStoreSnapshot did not reach sender");
        assertEquals(1, sentValues.size());
        assertEquals(event, sentValues.peek());
        assertArrayEquals(LongBytes.bigEndian(0xCAFEBABEL), sentKeys.peek(),
                "snapshot event must be keyed by itemId as 8 big-endian bytes");
    }

    @Test
    void publishPrivateStorePurchase_shouldNoOp_forNullEvent() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("privatestore", "acme.gs.events.privatestore"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPrivateStorePurchase(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishPrivateStoreSnapshot_shouldNoOp_forNullEvent() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(
                Collections.singletonMap("privatestore", "acme.gs.events.privatestore"),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPrivateStoreSnapshot(null);

        assertEquals(0, publisher.queueDepth());
        assertEquals(0L, publisher.droppedTotal());
    }

    @Test
    void publishPrivateStorePurchase_shouldShortCircuit_whenFamilyTopicMissing() {
        EventTypeRegistry registry = new EventTypeRegistry();
        publisher = new EventsPublisher(Collections.emptyMap(),
                (r, c) -> {
                }, cfg(5, 0L), registry);

        NxEventsImpl events = new NxEventsImpl(publisher, registry);
        events.publishPrivateStorePurchase(PrivateStorePurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
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
