package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MailDeletedEventTest {

    private static final UUID EVENT_ID = UUID.fromString("019a0000-0000-7000-8000-000000000001");

    @Test
    void builder_shouldMatchConstructor() {
        MailDeletedEvent fromBuilder = MailDeletedEvent.builder()
                .eventId(EVENT_ID).mailId(42L).side(MailDeletionSide.SENDER).build();
        MailDeletedEvent fromCtor = new MailDeletedEvent(EVENT_ID, 42L, MailDeletionSide.SENDER);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void equals_shouldDistinguishSide() {
        MailDeletedEvent sender = new MailDeletedEvent(EVENT_ID, 42L, MailDeletionSide.SENDER);
        MailDeletedEvent receiver = new MailDeletedEvent(EVENT_ID, 42L, MailDeletionSide.RECEIVER);

        assertNotEquals(sender, receiver);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        MailDeletedEvent original = new MailDeletedEvent(EVENT_ID, 7L, MailDeletionSide.RECEIVER);

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void readEvent_builder_shouldMatchConstructor() {
        MailReadEvent fromBuilder = MailReadEvent.builder().eventId(EVENT_ID).mailId(9L).build();
        MailReadEvent fromCtor = new MailReadEvent(EVENT_ID, 9L);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
        assertEquals(fromCtor, fromCtor.toBuilder().build());
    }
}
