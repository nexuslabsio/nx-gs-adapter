package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailAcceptedEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailCancelledEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailReturnedEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailSentEvent;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.OlympiadGameType;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.OlympiadMatchReason;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.OlympiadMatchResult;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.OlympiadMatchResultEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSide;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatetrade.PrivateTradeFinishedEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatetrade.TradeParty;
import app.l2nx.gs.adapter.api.kafka.events.raid.RaidBossKind;
import app.l2nx.gs.adapter.api.kafka.events.raid.RaidKillEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.commons.UUIDv7;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
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

    @Test
    void lookup_shouldReturnBinding_forRaidKillEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(RaidKillEvent.class);

        assertNotNull(binding);
        assertEquals("raid", binding.familyKey());
        assertEquals("RaidKillEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeBossNpcId_forRaidKillEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(RaidKillEvent.class);
        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUIDv7.generate())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(8, key.length);
        assertEquals(29028L, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void knownFamilies_shouldContainRaid() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("raid"));
    }

    @Test
    void lookup_shouldReturnBinding_forMailSentEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailSentEvent.class);

        assertNotNull(binding);
        assertEquals("mail", binding.familyKey());
        assertEquals("MailSentEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeMailId_forMailSentEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailSentEvent.class);
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUIDv7.generate())
                .mailId(0xCAFEBABEL)
                .senderCharId(0L).receiverCharId(2L)
                .subject("s").expiresAt(Instant.EPOCH).build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(8, key.length);
        assertEquals(0xCAFEBABEL, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void lookup_shouldReturnBinding_forMailAcceptedEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailAcceptedEvent.class);

        assertNotNull(binding);
        assertEquals("mail", binding.familyKey());
        assertEquals("MailAcceptedEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeMailId_forMailAcceptedEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailAcceptedEvent.class);
        MailAcceptedEvent event = MailAcceptedEvent.builder()
                .eventId(UUIDv7.generate()).mailId(0xBEEFL).claimedByCharId(1L).build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(0xBEEFL, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void lookup_shouldReturnBinding_forMailCancelledEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailCancelledEvent.class);

        assertNotNull(binding);
        assertEquals("mail", binding.familyKey());
        assertEquals("MailCancelledEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeMailId_forMailCancelledEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailCancelledEvent.class);
        MailCancelledEvent event = MailCancelledEvent.builder()
                .eventId(UUIDv7.generate()).mailId(0xBEEFL).cancelledByCharId(1L).build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(0xBEEFL, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void lookup_shouldReturnBinding_forMailReturnedEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailReturnedEvent.class);

        assertNotNull(binding);
        assertEquals("mail", binding.familyKey());
        assertEquals("MailReturnedEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeMailId_forMailReturnedEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(MailReturnedEvent.class);
        MailReturnedEvent event = MailReturnedEvent.builder()
                .eventId(UUIDv7.generate()).mailId(0xBEEFL).returnedToSenderId(1L).build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(0xBEEFL, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void knownFamilies_shouldContainMail() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("mail"));
    }

    @Test
    void lookup_shouldReturnBinding_forPrivateTradeFinishedEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateTradeFinishedEvent.class);

        assertNotNull(binding);
        assertEquals("privatetrade", binding.familyKey());
        assertEquals("PrivateTradeFinishedEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldReturnNull_forPrivateTradeFinishedEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(PrivateTradeFinishedEvent.class);
        TradeParty empty = TradeParty.builder().charId(1L).build();
        PrivateTradeFinishedEvent event = PrivateTradeFinishedEvent.builder()
                .eventId(UUIDv7.generate()).tradeId(UUIDv7.generate())
                .partyA(empty).partyB(empty).build();

        assertNull(binding.partitionKeyExtractor().apply(event));
    }

    @Test
    void knownFamilies_shouldContainPrivateTrade() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("privatetrade"));
    }

    @Test
    void lookup_shouldReturnBinding_forOlympiadMatchResultEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(OlympiadMatchResultEvent.class);

        assertNotNull(binding);
        assertEquals("olympiad", binding.familyKey());
        assertEquals("OlympiadMatchResultEvent", binding.messageType());
    }

    @Test
    void partitionKey_shouldEncodeCharId_forOlympiadMatchResultEvent() {
        EventTypeBinding binding = new EventTypeRegistry().lookup(OlympiadMatchResultEvent.class);
        OlympiadMatchResultEvent event = OlympiadMatchResultEvent.builder()
                .eventId(UUIDv7.generate())
                .matchId(UUIDv7.generate())
                .olympiadCycle(7)
                .gameType(OlympiadGameType.CLASSED)
                .charId(0xCAFEBABEL).classId(88)
                .opponentCharId(1L).opponentClassId(92)
                .result(OlympiadMatchResult.WIN)
                .reason(OlympiadMatchReason.NORMAL)
                .pointsBefore(40).pointsAfter(43)
                .build();

        byte[] key = binding.partitionKeyExtractor().apply(event);

        assertEquals(8, key.length);
        assertEquals(0xCAFEBABEL, ByteBuffer.wrap(key).getLong());
    }

    @Test
    void knownFamilies_shouldContainOlympiad() {
        assertTrue(new EventTypeRegistry().knownFamilies().contains("olympiad"));
    }
}
