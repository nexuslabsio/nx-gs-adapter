package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PrivateStorePurchaseEventTest {

    @Test
    void getLines_shouldReturnEmptyList_whenBuilderOmits() {
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .build();

        assertTrue(event.getLines().isEmpty());
    }

    @Test
    void getLines_shouldReturnEmptyList_whenBuilderPassesNull() {
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .lines(null)
                .build();

        assertTrue(event.getLines().isEmpty());
    }

    @Test
    void getSellerName_shouldBeNullable() {
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .build();

        assertNull(event.getSellerName());
        assertNull(event.getBuyerName());
    }

    @Test
    void getLines_shouldBeUnmodifiable() {
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .lines(Collections.singletonList(stubLine(1234L, 100L)))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getLines().add(stubLine(5678L, 200L)));
    }

    @Test
    void constructor_shouldDefensivelyCopyLinesList() {
        List<TradeLine> source = new ArrayList<TradeLine>();
        source.add(stubLine(1234L, 100L));

        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK)
                .sellerId(1L).buyerId(2L)
                .lines(source)
                .build();

        source.add(stubLine(5678L, 200L));

        assertEquals(1, event.getLines().size());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        PrivateStorePurchaseEvent original = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.BID)
                .sellerId(268437521L).sellerName("Hisho")
                .buyerId(268437522L).buyerName("Shanon")
                .lines(Arrays.asList(stubLine(1234L, 100L), stubLine(5678L, 200L)))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishEventId() {
        PrivateStorePurchaseEvent a = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK).sellerId(1L).buyerId(2L)
                .build();
        PrivateStorePurchaseEvent b = PrivateStorePurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .storeType(PrivateStoreSide.ASK).sellerId(1L).buyerId(2L)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishStoreType() {
        UUID id = UUID.randomUUID();
        PrivateStorePurchaseEvent ask = PrivateStorePurchaseEvent.builder()
                .eventId(id).storeType(PrivateStoreSide.ASK).sellerId(1L).buyerId(2L).build();
        PrivateStorePurchaseEvent bid = PrivateStorePurchaseEvent.builder()
                .eventId(id).storeType(PrivateStoreSide.BID).sellerId(1L).buyerId(2L).build();

        assertNotEquals(ask, bid);
    }

    @Test
    void toString_shouldRenderEventIdAndPartyIds() {
        UUID id = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
        PrivateStorePurchaseEvent event = PrivateStorePurchaseEvent.builder()
                .eventId(id).storeType(PrivateStoreSide.ASK)
                .sellerId(11L).buyerId(22L)
                .build();

        String s = event.toString();
        assertTrue(s.contains("eventId=" + id));
        assertTrue(s.contains("sellerId=11"));
        assertTrue(s.contains("buyerId=22"));
        assertTrue(s.contains("storeType=ASK"));
    }

    private static TradeLine stubLine(long itemId, long unitPrice) {
        return TradeLine.builder()
                .itemId(itemId).count(1L).unitPrice(unitPrice).currencyItemId(57L)
                .build();
    }
}
