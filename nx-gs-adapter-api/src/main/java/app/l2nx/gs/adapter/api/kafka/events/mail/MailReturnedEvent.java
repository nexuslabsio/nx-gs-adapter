package app.l2nx.gs.adapter.api.kafka.events.mail;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Inbox mail rejected (or auto-bounced on expiry) — bounces back to sender
 * as a new mail that surfaces as its own {@link MailSentEvent}.
 * {@link #getMailId() mailId} is the rejected mail's id, NOT the bounce.
 *
 * <ul>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about this return. {@code null} when absent.
 *   Hosts MAY add arbitrary keys without an API release; consumers
 *   ignore keys they do not understand.</li>
 * </ul>
 */
public final class MailReturnedEvent {

    private final UUID eventId;
    private final long mailId;
    private final long returnedToSenderId;
    private final List<MailItemMovement> attachments;
    private final @Nullable Map<String, String> metadata;

    public MailReturnedEvent(
            UUID eventId,
            long mailId,
            long returnedToSenderId,
            @Nullable List<MailItemMovement> attachments,
            @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.returnedToSenderId = returnedToSenderId;
        this.attachments = freezeList(attachments);
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
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

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .mailId(mailId)
                .returnedToSenderId(returnedToSenderId)
                .attachments(attachments)
                .metadata(metadata);
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
                && Objects.equals(attachments, that.attachments)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, returnedToSenderId, attachments, metadata);
    }

    @Override
    public String toString() {
        return "MailReturnedEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", returnedToSenderId=" + returnedToSenderId
                + ", attachments=" + attachments
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private long returnedToSenderId;
        private @Nullable List<MailItemMovement> attachments;
        private @Nullable Map<String, String> metadata;

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

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public MailReturnedEvent build() {
            return new MailReturnedEvent(eventId, mailId, returnedToSenderId, attachments, metadata);
        }
    }
}
