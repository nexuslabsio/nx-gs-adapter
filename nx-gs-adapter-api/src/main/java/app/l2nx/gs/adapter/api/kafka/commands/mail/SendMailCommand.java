package app.l2nx.gs.adapter.api.kafka.commands.mail;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to compose and deliver a system
 * mail to a single character. Replaces the legacy
 * {@code com.bohpts.messaging.dto.SendMailRequestV1} that bohpts received over
 * RabbitMQ on both the {@code admin-to-<server>} and {@code tg-to-<server>}
 * queues.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link SendMailPayload}{@code >} —
 * {@code success(payload)} carries the created mail-row primary keys (one per
 * mail; the host MAY split a single command into multiple mails when
 * attachments exceed its per-mail attachment cap) plus an optional per-line
 * partial-failure report. Common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — recipient {@code charId} does not exist on this
 *     server.</li>
 *     <li>{@code VALIDATION_FAILED} — required wire field missing
 *     ({@code charId} or {@code title}), or any {@link MailItem} in
 *     {@link #getItems() items} is malformed (null entry, null
 *     {@code itemTemplateId} or {@code count}, non-positive {@code count}).</li>
 *     <li>{@code INTERNAL_ERROR} — auto-emitted on handler
 *     {@code RuntimeException}; explicitly emitted by the handler for
 *     "shouldn't happen" branches (DB write failures, attachment-pipeline
 *     internal errors).</li>
 * </ul>
 *
 * <p><b>Required fields.</b> {@link #getCharId() charId} and
 * {@link #getTitle() title} are REQUIRED — the constructor enforces non-null
 * via {@link IllegalArgumentException} for programmatic construction.
 * {@link #getAuthor() author}, {@link #getBody() body}, and
 * {@link #getItems() items} are OPTIONAL — {@code null} {@code author} is
 * substituted by the host with its system-default sender name; {@code null}
 * {@code body} is treated as empty; {@code null} {@code items} is treated as
 * an empty attachment list (text-only mail).</p>
 *
 * <p>Wire-path deserialization bypasses the constructor via Gson — the handler
 * is responsible for null-checking {@code charId} / {@code title} and emitting
 * {@code VALIDATION_FAILED} when a wire field is missing.</p>
 *
 * <p><b>Partitioning.</b> Routed by {@link #getCharId() charId} on the
 * commands topic — sequential with other character-scoped operations on the
 * same recipient.</p>
 *
 * <p><b>Idempotency.</b> The handler MUST be idempotent — Kafka redelivery on
 * crash recovery may re-invoke the handler with the same
 * {@code Nx-Correlation-Id}. Best practice: cache recently-processed
 * correlation ids and short-circuit on a hit, replying with the original
 * {@link SendMailPayload}. Re-creating mails on every redelivery would result
 * in duplicate attachments grants, which is a real-money bug for paid
 * deliveries from the platform's commerce flows.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class SendMailCommand implements NxCommand<SendMailPayload> {

    private final Long charId;
    private final @Nullable String author;
    private final String title;
    private final @Nullable String body;
    private final List<MailItem> items;

    public SendMailCommand(Long charId,
                           @Nullable String author,
                           String title,
                           @Nullable String body,
                           @Nullable List<MailItem> items) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (title == null) {
            throw new IllegalArgumentException("title is required");
        }
        this.charId = charId;
        this.author = author;
        this.title = title;
        this.body = body;
        this.items = MailLists.freeze(items);
    }

    /**
     * Recipient character's primary key. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when the wire payload omits this field
     * (boxed {@code Long} surfaces missing wire data as {@code null}).
     */
    public Long getCharId() {
        return charId;
    }

    /**
     * Display name of the sender shown in the in-game mail UI. OPTIONAL —
     * when {@code null} (or blank in legacy semantics), the host substitutes
     * its system-default sender name. Free-form host-side string; NOT a
     * routing key.
     */
    public @Nullable String getAuthor() {
        return author;
    }

    /**
     * Mail subject. REQUIRED, non-blank semantically (the bohpts host's
     * underlying {@code MailManager} rejects blank titles with
     * {@link IllegalArgumentException}). Handler MUST emit
     * {@code VALIDATION_FAILED} on missing wire data; blank-title handling is
     * delegated to the host's mail-manager validation.
     */
    public String getTitle() {
        return title;
    }

    /**
     * Mail body text. OPTIONAL — {@code null} is treated as empty by the
     * host. Free-form host-supplied string.
     */
    public @Nullable String getBody() {
        return body;
    }

    /**
     * Attached items. Non-null on read; {@code null} passed to the constructor
     * is normalized to {@link Collections#emptyList()}. An empty list produces
     * a text-only mail.
     *
     * <p>Per-line validation (non-null entries, positive {@code count}) is
     * delegated to the handler — programmatic construction via
     * {@link MailItem#MailItem(Long, Long)} enforces it, but Gson-bypassed
     * wire-path entries MUST be re-checked.</p>
     */
    public List<MailItem> getItems() {
        return items;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .author(author)
                .title(title)
                .body(body)
                .items(items);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SendMailCommand)) return false;
        SendMailCommand that = (SendMailCommand) o;
        return Objects.equals(charId, that.charId)
                && Objects.equals(author, that.author)
                && Objects.equals(title, that.title)
                && Objects.equals(body, that.body)
                && Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, author, title, body, items);
    }

    @Override
    public String toString() {
        return "SendMailCommand[charId=" + charId
                + ", author=" + author
                + ", title=" + title
                + ", body=" + body
                + ", items=" + items + "]";
    }

    public static final class Builder {
        private Long charId;
        private @Nullable String author;
        private String title;
        private @Nullable String body;
        private @Nullable List<MailItem> items;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder author(@Nullable String author) {
            this.author = author;
            return this;
        }

        public Builder title(String title) {
            this.title = title;
            return this;
        }

        public Builder body(@Nullable String body) {
            this.body = body;
            return this;
        }

        public Builder items(@Nullable List<MailItem> items) {
            this.items = items;
            return this;
        }

        public SendMailCommand build() {
            return new SendMailCommand(charId, author, title, body, items);
        }
    }
}
