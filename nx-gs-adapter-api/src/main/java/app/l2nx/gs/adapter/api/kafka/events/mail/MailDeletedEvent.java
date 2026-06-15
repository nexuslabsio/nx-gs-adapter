package app.l2nx.gs.adapter.api.kafka.events.mail;

import java.util.Objects;
import java.util.UUID;

/**
 * One party deleted a mail from their view — source
 * {@code Message.setDeletedBySender()} / {@code setDeletedByReceiver()}.
 * {@link #getSide() side} distinguishes which flag flipped; sender and
 * receiver deletions are orthogonal (the same mail can fire both, in any
 * order), so this is a flag transition, not a lifecycle status change.
 *
 * <p>Keyed by {@link #getMailId() mailId} (8-byte BE) like the other mail
 * lifecycle events. The deletion moment derives from the UUIDv7
 * {@link #getEventId() eventId}.</p>
 */
public final class MailDeletedEvent {

    private final UUID eventId;
    private final long mailId;
    private final MailDeletionSide side;

    public MailDeletedEvent(UUID eventId, long mailId, MailDeletionSide side) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.side = side;
    }

    /**
     * UUIDv7 — upper 48 bits encode the deletion moment (occurredAt).
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Host-native {@code messages} row PK. Partition key (8-byte BE) shared
     * across all mail lifecycle events for this mail.
     */
    public long getMailId() {
        return mailId;
    }

    /**
     * Which party deleted the mail ({@code SENDER} / {@code RECEIVER}).
     */
    public MailDeletionSide getSide() {
        return side;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .mailId(mailId)
                .side(side);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MailDeletedEvent)) return false;
        MailDeletedEvent that = (MailDeletedEvent) o;
        return mailId == that.mailId
                && Objects.equals(eventId, that.eventId)
                && side == that.side;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, side);
    }

    @Override
    public String toString() {
        return "MailDeletedEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", side=" + side + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private MailDeletionSide side;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder mailId(long mailId) {
            this.mailId = mailId;
            return this;
        }

        public Builder side(MailDeletionSide side) {
            this.side = side;
            return this;
        }

        public MailDeletedEvent build() {
            return new MailDeletedEvent(eventId, mailId, side);
        }
    }
}
