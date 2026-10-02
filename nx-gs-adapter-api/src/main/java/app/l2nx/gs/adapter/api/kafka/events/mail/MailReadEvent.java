package app.l2nx.gs.adapter.api.kafka.events.mail;

import java.util.Objects;
import java.util.UUID;

/**
 * Receiver opened a mail for the first time. The reader is implicitly the receiver and the read time comes from the
 * UUIDv7 {@code eventId}. Keyed by {@code mailId} (8-byte BE) so it lands in the same partition as the other mail
 * lifecycle events, in occurrence order.
 */
public final class MailReadEvent {

    private final UUID eventId;
    private final long mailId;

    public MailReadEvent(UUID eventId, long mailId) {
        this.eventId = eventId;
        this.mailId = mailId;
    }

    public UUID getEventId() {
        return eventId;
    }

    /**
     * Host-native {@code messages} row PK; partition key (8-byte BE) shared by all mail lifecycle events of this mail.
     */
    public long getMailId() {
        return mailId;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).mailId(mailId);
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
