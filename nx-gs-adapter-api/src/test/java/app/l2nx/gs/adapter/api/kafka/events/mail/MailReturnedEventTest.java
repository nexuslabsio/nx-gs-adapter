package app.l2nx.gs.adapter.api.kafka.events.mail;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class MailReturnedEventTest {

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderOmits() {
        MailReturnedEvent event = MailReturnedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .returnedToSenderId(268437520L)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldReturnEmptyList_whenBuilderPassesNull() {
        MailReturnedEvent event = MailReturnedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .returnedToSenderId(268437520L)
                .attachments(null)
                .build();

        assertTrue(event.getAttachments().isEmpty());
    }

    @Test
    void getAttachments_shouldBeUnmodifiable() {
        MailReturnedEvent event = MailReturnedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .returnedToSenderId(268437520L)
                .attachments(Collections.singletonList(stub(1L)))
                .build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> event.getAttachments().add(stub(2L)));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttachmentsList() {
        List<MailItemMovement> source = new ArrayList<MailItemMovement>();
        source.add(stub(1L));

        MailReturnedEvent event = MailReturnedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .returnedToSenderId(268437520L)
                .attachments(source)
                .build();

        source.add(stub(2L));

        assertEquals(1, event.getAttachments().size());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        MailReturnedEvent original = MailReturnedEvent.builder()
                .eventId(UUID.randomUUID())
                .mailId(42L)
                .returnedToSenderId(268437520L)
                .attachments(Arrays.asList(stub(1L), stub(2L)))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishReturnedToSenderId() {
        UUID id = UUID.randomUUID();
        MailReturnedEvent a = MailReturnedEvent.builder()
                .eventId(id)
                .mailId(1L)
                .returnedToSenderId(10L)
                .build();
        MailReturnedEvent b = MailReturnedEvent.builder()
                .eventId(id)
                .mailId(1L)
                .returnedToSenderId(11L)
                .build();

        assertNotEquals(a, b);
    }

    private static MailItemMovement stub(long itemId) {
        return MailItemMovement.builder()
                .itemTemplateId(57L)
                .itemId(itemId)
                .newItemId(itemId)
                .count(1L)
                .build();
    }
}
