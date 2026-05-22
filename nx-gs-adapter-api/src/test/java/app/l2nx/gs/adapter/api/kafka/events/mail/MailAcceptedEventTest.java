package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MailAcceptedEventTest {

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderOmits() {
        MailAcceptedEvent event = MailAcceptedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .claimedByCharId(268437521L)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderPassesNull() {
        MailAcceptedEvent event = MailAcceptedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .claimedByCharId(268437521L)
                .attachments(null)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldBeUnmodifiable() {
        MailAcceptedEvent event = MailAcceptedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .claimedByCharId(268437521L)
                .attachments(Collections.singletonList(stub(1L, 2L)))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getAttachments().add(stub(3L, 4L)));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttachmentsList() {
        List<MailItemMovement> source = new ArrayList<MailItemMovement>();
        source.add(stub(1L, 2L));

        MailAcceptedEvent event = MailAcceptedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .claimedByCharId(268437521L)
                .attachments(source)
                .build();

        source.add(stub(3L, 4L));

        assertEquals(1, event.getAttachments().size());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        MailAcceptedEvent original = MailAcceptedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .claimedByCharId(268437521L)
                .attachments(Arrays.asList(stub(1L, 2L), stub(3L, 4L)))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishClaimedByCharId() {
        UUID id = UUID.randomUUID();
        MailAcceptedEvent a = MailAcceptedEvent.builder()
                .eventId(id).mailId(1L).claimedByCharId(10L).build();
        MailAcceptedEvent b = MailAcceptedEvent.builder()
                .eventId(id).mailId(1L).claimedByCharId(11L).build();

        assertNotEquals(a, b);
    }

    private static MailItemMovement stub(long itemId, long newItemId) {
        return MailItemMovement.builder()
                .itemTemplateId(57L).itemId(itemId).newItemId(newItemId).count(1L)
                .build();
    }
}
