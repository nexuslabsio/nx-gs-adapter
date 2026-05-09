package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.online.OnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSide;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreTradeEvent;
import app.l2nx.gs.commons.UUIDv7;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class EventTypeRegistryTest {

    @Test
    void lookup_shouldResolvePremiumPurchaseEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);

        assertNotNull(binding);
        assertEquals("premiumpurchase", binding.familyKey());
        assertEquals("PremiumPurchaseEvent", binding.messageType());
    }

    @Test
    void lookup_shouldReturnNull_forUnregisteredType() {
        assertNull(new EventTypeRegistry().lookup(String.class));
    }

    @Test
    void partitionKeyExtractor_shouldReturnCharacterIdAsLongBytes() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUIDv7.generate())
                .characterId(0xDEADBEEFL)
                .build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(8, key.length);
        long extracted = ByteBuffer.wrap(key).getLong();
        assertEquals(0xDEADBEEFL, extracted);
    }

    @Test
    void knownFamilies_shouldContainPremium() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("premiumpurchase"));
    }

    @Test
    void lookup_shouldResolveOnlineSnapshotEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(OnlineSnapshotEvent.class);

        assertNotNull(binding);
        assertEquals("serveronline", binding.familyKey());
        assertEquals("OnlineSnapshotEvent", binding.messageType());
    }

    @Test
    void partitionKeyExtractor_shouldReturnNull_forOnlineSnapshotEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(OnlineSnapshotEvent.class);
        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .buckets(Collections.singletonMap("total", 100L))
                .build();

        assertNull(binding.partitionKeyExtractor().apply(event));
    }

    @Test
    void knownFamilies_shouldContainOnline() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("serveronline"));
    }

    @Test
    void lookup_shouldResolvePrivateStoreTradeEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateStoreTradeEvent.class);

        assertNotNull(binding);
        assertEquals("privatestore", binding.familyKey());
        assertEquals("PrivateStoreTradeEvent", binding.messageType());
    }

    @Test
    void partitionKeyExtractor_shouldReturnNull_forPrivateStoreTradeEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateStoreTradeEvent.class);
        PrivateStoreTradeEvent event = PrivateStoreTradeEvent.builder()
                .eventId(UUIDv7.generate())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .build();

        assertNull(binding.partitionKeyExtractor().apply(event));
    }

    @Test
    void lookup_shouldResolvePrivateStoreSnapshotEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateStoreSnapshotEvent.class);

        assertNotNull(binding);
        assertEquals("privatestore", binding.familyKey());
        assertEquals("PrivateStoreSnapshotEvent", binding.messageType());
    }

    @Test
    void partitionKeyExtractor_shouldReturnItemIdAsLongBytes_forPrivateStoreSnapshotEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateStoreSnapshotEvent.class);
        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .itemId(0xDEADBEEFL)
                .side(PrivateStoreSide.ASK)
                .build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(8, key.length);
        long extracted = ByteBuffer.wrap(key).getLong();
        assertEquals(0xDEADBEEFL, extracted);
    }

    @Test
    void knownFamilies_shouldContainPrivateStore() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("privatestore"));
    }
}
