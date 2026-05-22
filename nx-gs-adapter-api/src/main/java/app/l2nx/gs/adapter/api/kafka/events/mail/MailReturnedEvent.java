package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Inbox mail rejected (or auto-bounced on expiry) — bounces back to sender
 * as a new mail that surfaces as its own {@link MailSentEvent}.
 * {@link #getMailId() mailId} is the rejected mail's id, NOT the bounce.
 */
public final class MailReturnedEvent {

    private final UUID eventId;
    private final long mailId;
    private final long returnedToSenderId;
    private final List<MailItemMovement> attachments;

    public MailReturnedEvent(UUID eventId,
                             long mailId,
                             long returnedToSenderId,
                             @Nullable List<MailItemMovement> attachments) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.returnedToSenderId = returnedToSenderId;
        this.attachments = freezeList(attachments);
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getMailId() {
        return mailId;
    }

    public long getReturnedToSenderId() {
        return returnedToSenderId;
    }

    public List<MailItemMovement> getAttachments() {
        return attachments;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .mailId(mailId)
                .returnedToSenderId(returnedToSenderId)
                .attachments(attachments);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<MailItemMovement> freezeList(@Nullable List<MailItemMovement> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<MailItemMovement>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MailReturnedEvent)) return false;
        MailReturnedEvent that = (MailReturnedEvent) o;
        return mailId == that.mailId
                && returnedToSenderId == that.returnedToSenderId
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(attachments, that.attachments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, returnedToSenderId, attachments);
    }

    @Override
    public String toString() {
        return "MailReturnedEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", returnedToSenderId=" + returnedToSenderId
                + ", attachments=" + attachments + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private long returnedToSenderId;
        private @Nullable List<MailItemMovement> attachments;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder mailId(long mailId) {
            this.mailId = mailId;
            return this;
        }

        public Builder returnedToSenderId(long returnedToSenderId) {
            this.returnedToSenderId = returnedToSenderId;
            return this;
        }

        public Builder attachments(@Nullable List<MailItemMovement> attachments) {
            this.attachments = attachments;
            return this;
        }

        public MailReturnedEvent build() {
            return new MailReturnedEvent(eventId, mailId, returnedToSenderId, attachments);
        }
    }
}
