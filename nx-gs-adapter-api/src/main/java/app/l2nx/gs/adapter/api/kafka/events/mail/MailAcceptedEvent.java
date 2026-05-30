package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Receiver claimed mail attachments. COD adena movement (if any) is
 * inferred consumer-side from the paired SENT event's
 * {@link MailSentEvent#getCodAmount() codAmount}.
 *
 * <ul>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about this mail claim. {@code null} when absent;
 *   hosts MAY add arbitrary keys without an API release.</li>
 * </ul>
 */
public final class MailAcceptedEvent {

    private final UUID eventId;
    private final long mailId;
    private final long claimedByCharId;
    private final List<MailItemMovement> attachments;
    private final @Nullable Map<String, String> metadata;

    public MailAcceptedEvent(UUID eventId,
                             long mailId,
                             long claimedByCharId,
                             @Nullable List<MailItemMovement> attachments,
                             @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.claimedByCharId = claimedByCharId;
        this.attachments = freezeList(attachments);
        this.metadata = metadata == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
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

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .mailId(mailId)
                .claimedByCharId(claimedByCharId)
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
        if (!(o instanceof MailAcceptedEvent)) return false;
        MailAcceptedEvent that = (MailAcceptedEvent) o;
        return mailId == that.mailId
                && claimedByCharId == that.claimedByCharId
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(attachments, that.attachments)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, claimedByCharId, attachments, metadata);
    }

    @Override
    public String toString() {
        return "MailAcceptedEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", claimedByCharId=" + claimedByCharId
                + ", attachments=" + attachments
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private long claimedByCharId;
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

        public Builder claimedByCharId(long claimedByCharId) {
            this.claimedByCharId = claimedByCharId;
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

        public MailAcceptedEvent build() {
            return new MailAcceptedEvent(eventId, mailId, claimedByCharId, attachments, metadata);
        }
    }
}
