package app.l2nx.gs.adapter.api.kafka.commands.mail;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.mail.model.MailItem;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Composes and delivers a system mail to one character.
 *
 * <p>Success carries the created mail ids (the host MAY split into several mails over its per-mail attachment cap)
 * plus optional per-line failures. {@code NOT_FOUND}: unknown recipient. {@code VALIDATION_FAILED}: missing
 * {@code charId}/{@code title} or a malformed {@link MailItem}; Gson bypasses the constructor, so the handler
 * must check. {@code INTERNAL_ERROR}: handler exception or host-side failure.</p>
 *
 * <p>Optional: {@code null} author becomes the host's default sender, {@code null} body is empty, {@code null} items
 * is a text-only mail. Routed by {@code charId}.</p>
 *
 * <p>Delivery is at-most-once (see {@link app.l2nx.gs.adapter.api.spi.capability.CommandHandler}); a re-issue after a reply timeout looks like a fresh send
 * and mails and grants twice, a real-money bug for paid deliveries, so the caller MUST establish whether the first
 * send landed before re-issuing.</p>
 */
public final class SendMailCommand implements NxCommand<SendMailResult> {

    private final Long charId;
    private final @Nullable String author;
    private final String title;
    private final @Nullable String body;
    private final List<MailItem> items;

    public SendMailCommand(
            Long charId, @Nullable String author, String title, @Nullable String body, @Nullable List<MailItem> items) {
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

    public Long getCharId() {
        return charId;
    }

    /**
     * {@code null} falls back to the host's system-default sender name. Display only, not a routing key.
     */
    public @Nullable String getAuthor() {
        return author;
    }

    /** The host's {@code MailManager} rejects blank titles. */
    public String getTitle() {
        return title;
    }

    public @Nullable String getBody() {
        return body;
    }

    /**
     * Non-null; {@code null} is normalized to empty (text-only mail). Gson-bypassed entries need per-line re-validation by the handler.
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
