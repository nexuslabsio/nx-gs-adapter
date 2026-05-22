package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MailSentEventTest {

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderOmits() {
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(0L)
                .receiverCharId(268437521L)
                .subject("Reward")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderPassesNull() {
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(0L)
                .receiverCharId(268437521L)
                .subject("Reward")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .attachments(null)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getSenderName_shouldBeNullable_forPlayerToPlayerMail() {
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(268437520L)
                .receiverCharId(268437521L)
                .subject("Hi")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .build();

        assertNull(event.getSenderName());
    }

    @Test
    void getBody_shouldBeNullable_forSystemMailWithoutBody() {
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(0L)
                .senderName("System")
                .receiverCharId(268437521L)
                .subject("Reward")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .build();

        assertNull(event.getBody());
    }

    @Test
    void getCodAmount_shouldDefaultToZero() {
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(268437520L)
                .receiverCharId(268437521L)
                .subject("Trade")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .build();

        assertEquals(0L, event.getCodAmount());
    }

    @Test
    void getAttachments_shouldBeUnmodifiable() {
        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(0L)
                .receiverCharId(268437521L)
                .subject("Reward")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .attachments(Collections.singletonList(stub()))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getAttachments().add(stub()));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttachmentsList() {
        List<MailItemMovement> source = new ArrayList<MailItemMovement>();
        source.add(stub());

        MailSentEvent event = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(0L)
                .receiverCharId(268437521L)
                .subject("Reward")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .attachments(source)
                .build();

        source.add(stub());

        assertEquals(1, event.getAttachments().size());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        MailSentEvent original = MailSentEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .senderCharId(268437520L)
                .senderName("Olya")
                .receiverCharId(268437521L)
                .subject("Subject")
                .body("Body")
                .expiresAt(Instant.parse("2026-06-01T12:00:00Z"))
                .codAmount(1_000_000L)
                .attachments(Arrays.asList(stub(), stub()))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishCodAmount() {
        UUID id = UUID.randomUUID();
        MailSentEvent a = MailSentEvent.builder().eventId(id).mailId(1L)
                .senderCharId(0L).receiverCharId(2L).subject("s")
                .expiresAt(Instant.EPOCH).codAmount(0L).build();
        MailSentEvent b = MailSentEvent.builder().eventId(id).mailId(1L)
                .senderCharId(0L).receiverCharId(2L).subject("s")
                .expiresAt(Instant.EPOCH).codAmount(100L).build();

        assertNotEquals(a, b);
    }

    @Test
    void toString_shouldRenderMailIdAndParties() {
        UUID id = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
        MailSentEvent event = MailSentEvent.builder()
                .eventId(id).mailId(99L)
                .senderCharId(11L).receiverCharId(22L)
                .subject("s").expiresAt(Instant.EPOCH).build();

        String s = event.toString();
        assertTrue(s.contains("mailId=99"));
        assertTrue(s.contains("senderCharId=11"));
        assertTrue(s.contains("receiverCharId=22"));
    }

    private static MailItemMovement stub() {
        return MailItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(1L).count(1L)
                .build();
    }
}
