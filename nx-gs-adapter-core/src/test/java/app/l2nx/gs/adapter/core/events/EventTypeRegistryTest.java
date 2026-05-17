package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSide;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
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
    void knownFamilies_shouldContainPremiumPurchase() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("premiumpurchase"));
    }

    @Test
    void lookup_shouldResolveServerOnlineSnapshotEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(ServerOnlineSnapshotEvent.class);

        assertNotNull(binding);
        assertEquals("serveronline", binding.familyKey());
        assertEquals("ServerOnlineSnapshotEvent", binding.messageType());
    }

    @Test
    void partitionKeyExtractor_shouldReturnNull_forServerOnlineSnapshotEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(ServerOnlineSnapshotEvent.class);
        ServerOnlineSnapshotEvent event = ServerOnlineSnapshotEvent.builder()
                .eventId(UUIDv7.generate())
                .buckets(Collections.singletonMap("total", 100L))
                .build();

        assertNull(binding.partitionKeyExtractor().apply(event));
    }

    @Test
    void knownFamilies_shouldContainServerOnline() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("serveronline"));
    }

    @Test
    void lookup_shouldResolvePrivateStorePurchaseEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateStorePurchaseEvent.class);

        assertNotNull(binding);
        assertEquals("privatestore", binding.familyKey());
        assertEquals("PrivateStorePurchaseEvent", binding.messageType());
    }

    @Test
    void partitionKeyExtractor_shouldReturnNull_forPrivateStorePurchaseEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateStorePurchaseEvent.class);
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
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

    @Test
    void lookup_shouldReturnBinding_forCharacterPresenceEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(CharacterPresenceEvent.class);

        assertNotNull(binding);
        assertEquals("character", binding.familyKey());
        assertEquals("CharacterPresenceEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeCharId_forCharacterPresenceEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(CharacterPresenceEvent.class);
        CharacterPresenceEvent event = CharacterPresenceEvent.builder()
                .eventId(UUIDv7.generate())
                .charId(0xCAFEBABEL)
                .online(true)
                .build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(8, key.length);
        assertEquals(0xCAFEBABEL, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void knownFamilies_shouldContainCharacter() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("character"));
    }
}
