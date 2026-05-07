package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumPurchaseEvent;
import app.l2nx.gs.commons.UUIDv7;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class EventTypeRegistryTest {

    @Test
    void lookup_shouldResolvePremiumPurchaseEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PremiumPurchaseEvent.class);

        assertNotNull(binding);
        assertEquals("premium", binding.familyKey());
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
        assertTrue(new EventTypeRegistry().knownFamilies().contains("premium"));
    }
}
