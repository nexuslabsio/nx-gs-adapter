package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PrivateStoreSnapshotEventTest {

    @Test
    void getOffers_shouldReturnEmptyList_whenBuilderOmits() {
        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .itemId(1234L)
                .side(PrivateStoreSide.ASK)
                .build();

        assertTrue(event.getOffers().isEmpty());
    }

    @Test
    void getOffers_shouldReturnEmptyList_whenBuilderPassesNull() {
        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .itemId(1234L)
                .side(PrivateStoreSide.ASK)
                .offers(null)
                .build();

        // Empty offers list is the documented tombstone shape.
        assertTrue(event.getOffers().isEmpty());
    }

    @Test
    void getOffers_shouldBeUnmodifiable() {
        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .itemId(1234L)
                .side(PrivateStoreSide.ASK)
                .offers(Collections.singletonList(stubOffer(1L, 100L)))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getOffers().add(stubOffer(2L, 200L)));
    }

    @Test
    void constructor_shouldDefensivelyCopyOffersList() {
        List<Offer> source = new ArrayList<Offer>();
        source.add(stubOffer(1L, 100L));

        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .itemId(1234L).side(PrivateStoreSide.ASK)
                .offers(source)
                .build();

        source.add(stubOffer(2L, 200L));

        assertEquals(1, event.getOffers().size());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        PrivateStoreSnapshotEvent original = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .itemId(6364L)
                .side(PrivateStoreSide.BID)
                .offers(Arrays.asList(
                        stubOffer(1L, 100L),
                        stubOffer(2L, 200L)))
                .build();

        PrivateStoreSnapshotEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishEventId() {
        PrivateStoreSnapshotEvent a = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID()).itemId(1L).side(PrivateStoreSide.ASK).build();
        PrivateStoreSnapshotEvent b = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID()).itemId(1L).side(PrivateStoreSide.ASK).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishItemId() {
        UUID id = UUID.randomUUID();
        PrivateStoreSnapshotEvent a = PrivateStoreSnapshotEvent.builder()
                .eventId(id).itemId(1L).side(PrivateStoreSide.ASK).build();
        PrivateStoreSnapshotEvent b = PrivateStoreSnapshotEvent.builder()
                .eventId(id).itemId(2L).side(PrivateStoreSide.ASK).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishSide() {
        UUID id = UUID.randomUUID();
        PrivateStoreSnapshotEvent ask = PrivateStoreSnapshotEvent.builder()
                .eventId(id).itemId(1L).side(PrivateStoreSide.ASK).build();
        PrivateStoreSnapshotEvent bid = PrivateStoreSnapshotEvent.builder()
                .eventId(id).itemId(1L).side(PrivateStoreSide.BID).build();

        assertNotEquals(ask, bid);
    }

    @Test
    void emptyOffers_shouldBeAcceptedAsTombstone() {
        // Tombstone semantics: producers emit one empty-offers event when a
        // tracked (itemId, side) pair empties. The DTO must permit it.
        PrivateStoreSnapshotEvent tombstone = PrivateStoreSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .itemId(1234L).side(PrivateStoreSide.ASK)
                .offers(Collections.emptyList())
                .build();

        assertTrue(tombstone.getOffers().isEmpty());
    }

    @Test
    void toString_shouldRenderEventIdItemIdAndSide() {
        UUID id = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
        PrivateStoreSnapshotEvent event = PrivateStoreSnapshotEvent.builder()
                .eventId(id).itemId(6364L).side(PrivateStoreSide.BID)
                .build();

        String s = event.toString();
        assertTrue(s.contains("eventId=" + id));
        assertTrue(s.contains("itemId=6364"));
        assertTrue(s.contains("side=BID"));
    }

    private static Offer stubOffer(long traderId, long unitPrice) {
        return Offer.builder()
                .traderId(traderId).count(1L).unitPrice(unitPrice).currencyItemId(57L)
                .build();
    }
}
