package app.l2nx.gs.adapter.api.kafka.events.mail;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class MailCancelledEventTest {

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderOmits() {
        MailCancelledEvent event = MailCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .cancelledByCharId(268437521L)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderPassesNull() {
        MailCancelledEvent event = MailCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .cancelledByCharId(268437521L)
                .attachments(null)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldBeUnmodifiable() {
        MailCancelledEvent event = MailCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .cancelledByCharId(268437521L)
                .attachments(Collections.singletonList(stub(1L, 2L)))
                .build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> event.getAttachments().add(stub(3L, 4L)));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttachmentsList() {
        List<MailItemMovement> source = new ArrayList<MailItemMovement>();
        source.add(stub(1L, 2L));

        MailCancelledEvent event = MailCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .cancelledByCharId(268437521L)
                .attachments(source)
                .build();

        source.add(stub(3L, 4L));

        assertEquals(1, event.getAttachments().size());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        MailCancelledEvent original = MailCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .cancelledByCharId(268437521L)
                .attachments(Arrays.asList(stub(1L, 2L), stub(3L, 4L)))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishCancelledByCharId() {
        UUID id = UUID.randomUUID();
        MailCancelledEvent a = MailCancelledEvent.builder()
                .eventId(id)
                .mailId(1L)
                .cancelledByCharId(10L)
                .build();
        MailCancelledEvent b = MailCancelledEvent.builder()
                .eventId(id)
                .mailId(1L)
                .cancelledByCharId(11L)
                .build();

        assertNotEquals(a, b);
    }

    private static MailItemMovement stub(long itemId, long newItemId) {
        return MailItemMovement.builder()
                .itemTemplateId(57L)
                .itemId(itemId)
                .newItemId(newItemId)
                .count(1L)
                .build();
    }
}
