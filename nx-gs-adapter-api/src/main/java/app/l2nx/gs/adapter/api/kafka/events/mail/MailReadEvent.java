package app.l2nx.gs.adapter.api.kafka.events.mail;

import java.util.Objects;
import java.util.UUID;

/**
 * Receiver opened (read) a mail for the first time — source
 * {@code Message.markAsRead()}. The reader is implicitly the receiver; the
 * read moment derives from the UUIDv7 {@link #getEventId() eventId}, so no
 * separate {@code readAt} rides the wire.
 *
 * <p>Keyed by {@link #getMailId() mailId} (8-byte BE) like the other mail
 * lifecycle events, so the read fact lands in the same partition in
 * occurrence order.</p>
 */
public final class MailReadEvent {

    private final UUID eventId;
    private final long mailId;

    public MailReadEvent(UUID eventId, long mailId) {
        this.eventId = eventId;
        this.mailId = mailId;
    }

    /**
     * UUIDv7 — upper 48 bits encode the read moment (occurredAt).
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

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .mailId(mailId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MailReadEvent)) return false;
        MailReadEvent that = (MailReadEvent) o;
        return mailId == that.mailId && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId);
    }

    @Override
    public String toString() {
        return "MailReadEvent[eventId=" + eventId + ", mailId=" + mailId + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder mailId(long mailId) {
            this.mailId = mailId;
            return this;
        }

        public MailReadEvent build() {
            return new MailReadEvent(eventId, mailId);
        }
    }
}
