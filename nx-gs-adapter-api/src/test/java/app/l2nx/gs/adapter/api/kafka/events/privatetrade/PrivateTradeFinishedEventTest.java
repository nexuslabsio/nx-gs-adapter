package app.l2nx.gs.adapter.api.kafka.events.privatetrade;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PrivateTradeFinishedEventTest {

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        UUID eventId = UUID.randomUUID();
        UUID tradeId = UUID.randomUUID();
        TradeParty a = TradeParty.builder()
                .charId(268437521L)
                .items(Collections.singletonList(stub(57L, 1L, 1L, 1_000_000L)))
                .build();
        TradeParty b = TradeParty.builder()
                .charId(268437522L)
                .items(Collections.singletonList(stub(6611L, 11L, 22L, 1L)))
                .build();

        PrivateTradeFinishedEvent original = PrivateTradeFinishedEvent.builder()
                .eventId(eventId)
                .tradeId(tradeId)
                .partyA(a)
                .partyB(b)
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishEventId() {
        UUID tradeId = UUID.randomUUID();
        TradeParty empty = TradeParty.builder().charId(1L).build();

        PrivateTradeFinishedEvent a = PrivateTradeFinishedEvent.builder()
                .eventId(UUID.randomUUID())
                .tradeId(tradeId)
                .partyA(empty)
                .partyB(empty)
                .build();
        PrivateTradeFinishedEvent b = PrivateTradeFinishedEvent.builder()
                .eventId(UUID.randomUUID())
                .tradeId(tradeId)
                .partyA(empty)
                .partyB(empty)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishTradeId() {
        UUID eventId = UUID.randomUUID();
        TradeParty empty = TradeParty.builder().charId(1L).build();

        PrivateTradeFinishedEvent a = PrivateTradeFinishedEvent.builder()
                .eventId(eventId)
                .tradeId(UUID.randomUUID())
                .partyA(empty)
                .partyB(empty)
                .build();
        PrivateTradeFinishedEvent b = PrivateTradeFinishedEvent.builder()
                .eventId(eventId)
                .tradeId(UUID.randomUUID())
                .partyA(empty)
                .partyB(empty)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void giftTrade_shouldRepresentEmptySideAsEmptyItemsList() {
        TradeParty gifter = TradeParty.builder()
                .charId(1L)
                .items(Collections.singletonList(stub(6611L, 11L, 22L, 1L)))
                .build();
        TradeParty receiver = TradeParty.builder().charId(2L).build();

        PrivateTradeFinishedEvent event = PrivateTradeFinishedEvent.builder()
                .eventId(UUID.randomUUID())
                .tradeId(UUID.randomUUID())
                .partyA(gifter)
                .partyB(receiver)
                .build();

        assertEquals(1, event.getPartyA().getItems().size());
        assertTrue(event.getPartyB().getItems().isEmpty());
    }

    @Test
    void toString_shouldRenderIds() {
        UUID eventId = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
        UUID tradeId = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000001");
        TradeParty empty = TradeParty.builder().charId(11L).build();

        PrivateTradeFinishedEvent event = PrivateTradeFinishedEvent.builder()
                .eventId(eventId)
                .tradeId(tradeId)
                .partyA(empty)
                .partyB(empty)
                .build();

        String s = event.toString();
        assertTrue(s.contains("eventId=" + eventId));
        assertTrue(s.contains("tradeId=" + tradeId));
    }

    private static TradeItemMovement stub(long template, long itemId, long newItemId, long count) {
        return TradeItemMovement.builder()
                .itemTemplateId(template)
                .itemId(itemId)
                .newItemId(newItemId)
                .count(count)
                .build();
    }
}
