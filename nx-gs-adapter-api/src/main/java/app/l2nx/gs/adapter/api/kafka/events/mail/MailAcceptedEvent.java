package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Receiver claimed mail attachments. COD adena movement (if any) is
 * inferred consumer-side from the paired SENT event's
 * {@link MailSentEvent#getCodAmount() codAmount}.
 */
public final class MailAcceptedEvent {

    private final UUID eventId;
    private final long mailId;
    private final long claimedByCharId;
    private final List<MailItemMovement> attachments;

    public MailAcceptedEvent(UUID eventId,
                             long mailId,
                             long claimedByCharId,
                             @Nullable List<MailItemMovement> attachments) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.claimedByCharId = claimedByCharId;
        this.attachments = freezeList(attachments);
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getMailId() {
        return mailId;
    }

    public long getClaimedByCharId() {
        return claimedByCharId;
    }

    public List<MailItemMovement> getAttachments() {
        return attachments;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .mailId(mailId)
                .claimedByCharId(claimedByCharId)
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
        if (!(o instanceof MailAcceptedEvent)) return false;
        MailAcceptedEvent that = (MailAcceptedEvent) o;
        return mailId == that.mailId
                && claimedByCharId == that.claimedByCharId
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(attachments, that.attachments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, claimedByCharId, attachments);
    }

    @Override
    public String toString() {
        return "MailAcceptedEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", claimedByCharId=" + claimedByCharId
                + ", attachments=" + attachments + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private long claimedByCharId;
        private @Nullable List<MailItemMovement> attachments;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder mailId(long mailId) {
            this.mailId = mailId;
            return this;
        }

        public Builder claimedByCharId(long claimedByCharId) {
            this.claimedByCharId = claimedByCharId;
            return this;
        }

        public Builder attachments(@Nullable List<MailItemMovement> attachments) {
            this.attachments = attachments;
            return this;
        }

        public MailAcceptedEvent build() {
            return new MailAcceptedEvent(eventId, mailId, claimedByCharId, attachments);
        }
    }
}
