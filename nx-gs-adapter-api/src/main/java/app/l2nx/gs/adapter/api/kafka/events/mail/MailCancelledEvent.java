package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Sender cancelled an outbox mail before the receiver claimed it —
 * attachments are pulled back to the sender and the mail row is deleted.
 *
 * <ul>
 *   <li>{@code metadata} — optional open string→string map of build-agnostic
 *       attributes. {@code null} when absent. Hosts MAY add
 *       arbitrary keys without an API release.</li>
 * </ul>
 */
public final class MailCancelledEvent {

    private final UUID eventId;
    private final long mailId;
    private final long cancelledByCharId;
    private final List<MailItemMovement> attachments;
    private final @Nullable Map<String, String> metadata;

    public MailCancelledEvent(UUID eventId,
                              long mailId,
                              long cancelledByCharId,
                              @Nullable List<MailItemMovement> attachments,
                              @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.cancelledByCharId = cancelledByCharId;
        this.attachments = freezeList(attachments);
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getMailId() {
        return mailId;
    }

    public long getCancelledByCharId() {
        return cancelledByCharId;
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
                .cancelledByCharId(cancelledByCharId)
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
        if (!(o instanceof MailCancelledEvent)) return false;
        MailCancelledEvent that = (MailCancelledEvent) o;
        return mailId == that.mailId
                && cancelledByCharId == that.cancelledByCharId
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(attachments, that.attachments)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, cancelledByCharId, attachments, metadata);
    }

    @Override
    public String toString() {
        return "MailCancelledEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", cancelledByCharId=" + cancelledByCharId
                + ", attachments=" + attachments
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private long cancelledByCharId;
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

        public Builder cancelledByCharId(long cancelledByCharId) {
            this.cancelledByCharId = cancelledByCharId;
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

        public MailCancelledEvent build() {
            return new MailCancelledEvent(eventId, mailId, cancelledByCharId, attachments, metadata);
        }
    }
}
