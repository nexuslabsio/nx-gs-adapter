package app.l2nx.gs.adapter.api.kafka.events.mail;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.*;

/**
 * Mail row created on the host. One event per row; batched sends emit one
 * event per resulting row, each with its own {@link #getMailId() mailId}.
 * Bounced-back rejected mails (see {@link MailReturnedEvent}) also surface
 * here as a fresh SENT with the bounce mail's new {@code mailId}.
 *
 * <p>{@link #getSubject() subject} and {@link #getBody() body} are plaintext
 * — treat the topic as sensitive.</p>
 *
 * <ul>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about this mail. {@code null} when absent.
 *   Hosts MAY add arbitrary keys without an API release; consumers
 *   ignore keys they do not understand.</li>
 * </ul>
 */
public final class MailSentEvent {

    private final UUID eventId;
    private final long mailId;
    private final long senderCharId;
    private final @Nullable String senderName;
    private final long receiverCharId;
    private final String subject;
    private final @Nullable String body;
    private final Instant expiresAt;
    private final long codAmount;
    private final List<MailItemMovement> attachments;
    private final @Nullable Map<String, String> metadata;

    public MailSentEvent(UUID eventId,
                         long mailId,
                         long senderCharId,
                         @Nullable String senderName,
                         long receiverCharId,
                         String subject,
                         @Nullable String body,
                         Instant expiresAt,
                         long codAmount,
                         @Nullable List<MailItemMovement> attachments,
                         @Nullable Map<String, String> metadata) {
        this.eventId = eventId;
        this.mailId = mailId;
        this.senderCharId = senderCharId;
        this.senderName = senderName;
        this.receiverCharId = receiverCharId;
        this.subject = subject;
        this.body = body;
        this.expiresAt = expiresAt;
        this.codAmount = codAmount;
        this.attachments = freezeList(attachments);
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * UUIDv7 — upper 48 bits encode occurredAt.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Host-native {@code messages} row PK. Partition key (8-byte BE) shared
     * across all four lifecycle events for this mail.
     */
    public long getMailId() {
        return mailId;
    }

    /**
     * Sender char id. {@code 0} for system / NPC mail (see
     * {@link #getSenderName()}).
     */
    public long getSenderCharId() {
        return senderCharId;
    }

    /**
     * Display author when set by host (system / NPC mail).
     * {@code null} for player-to-player — name comes from char CDC.
     */
    public @Nullable String getSenderName() {
        return senderName;
    }

    public long getReceiverCharId() {
        return receiverCharId;
    }

    public String getSubject() {
        return subject;
    }

    public @Nullable String getBody() {
        return body;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    /**
     * COD adena required from receiver on accept. {@code 0} = none.
     */
    public long getCodAmount() {
        return codAmount;
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
                .senderCharId(senderCharId)
                .senderName(senderName)
                .receiverCharId(receiverCharId)
                .subject(subject)
                .body(body)
                .expiresAt(expiresAt)
                .codAmount(codAmount)
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
        if (!(o instanceof MailSentEvent)) return false;
        MailSentEvent that = (MailSentEvent) o;
        return mailId == that.mailId
                && senderCharId == that.senderCharId
                && receiverCharId == that.receiverCharId
                && codAmount == that.codAmount
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(senderName, that.senderName)
                && Objects.equals(subject, that.subject)
                && Objects.equals(body, that.body)
                && Objects.equals(expiresAt, that.expiresAt)
                && Objects.equals(attachments, that.attachments)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, mailId, senderCharId, senderName, receiverCharId,
                subject, body, expiresAt, codAmount, attachments, metadata);
    }

    @Override
    public String toString() {
        return "MailSentEvent[eventId=" + eventId
                + ", mailId=" + mailId
                + ", senderCharId=" + senderCharId
                + ", senderName=" + senderName
                + ", receiverCharId=" + receiverCharId
                + ", subject=" + subject
                + ", body=" + body
                + ", expiresAt=" + expiresAt
                + ", codAmount=" + codAmount
                + ", attachments=" + attachments
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private UUID eventId;
        private long mailId;
        private long senderCharId;
        private @Nullable String senderName;
        private long receiverCharId;
        private String subject;
        private @Nullable String body;
        private Instant expiresAt;
        private long codAmount;
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

        public Builder senderCharId(long senderCharId) {
            this.senderCharId = senderCharId;
            return this;
        }

        public Builder senderName(@Nullable String senderName) {
            this.senderName = senderName;
            return this;
        }

        public Builder receiverCharId(long receiverCharId) {
            this.receiverCharId = receiverCharId;
            return this;
        }

        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder body(@Nullable String body) {
            this.body = body;
            return this;
        }

        public Builder expiresAt(Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public Builder codAmount(long codAmount) {
            this.codAmount = codAmount;
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

        public MailSentEvent build() {
            return new MailSentEvent(eventId, mailId, senderCharId, senderName, receiverCharId,
                    subject, body, expiresAt, codAmount, attachments, metadata);
        }
    }
}
