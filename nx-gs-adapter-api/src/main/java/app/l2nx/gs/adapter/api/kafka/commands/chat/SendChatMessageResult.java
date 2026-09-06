package app.l2nx.gs.adapter.api.kafka.commands.chat;

import java.util.Objects;

/**
 * Success payload of {@link SendChatMessageCommand}. Pure telemetry — the
 * platform stores the message from its own echo event, not from this reply, so
 * neither field is load-bearing for correctness.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class SendChatMessageResult {

    private final int linesSent;
    private final int recipients;

    public SendChatMessageResult(int linesSent, int recipients) {
        this.linesSent = linesSent;
        this.recipients = recipients;
    }

    /**
     * Physical chat lines emitted — the count of non-empty lines after the host
     * splits {@link SendChatMessageCommand#getText()} on {@code \n}.
     */
    public int getLinesSent() {
        return linesSent;
    }

    /**
     * Online recipients the packet actually reached. Best-effort; hosts that do
     * not track this MAY report {@code 0}, so a zero here does NOT mean the
     * message failed.
     */
    public int getRecipients() {
        return recipients;
    }

    public Builder toBuilder() {
        return new Builder().linesSent(linesSent).recipients(recipients);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SendChatMessageResult)) return false;
        SendChatMessageResult that = (SendChatMessageResult) o;
        return linesSent == that.linesSent && recipients == that.recipients;
    }

    @Override
    public int hashCode() {
        return Objects.hash(linesSent, recipients);
    }

    @Override
    public String toString() {
        return "SendChatMessageResult[linesSent=" + linesSent + ", recipients=" + recipients + "]";
    }

    public static final class Builder {
        private int linesSent;
        private int recipients;

        public Builder linesSent(int linesSent) {
            this.linesSent = linesSent;
            return this;
        }

        public Builder recipients(int recipients) {
            this.recipients = recipients;
            return this;
        }

        public SendChatMessageResult build() {
            return new SendChatMessageResult(linesSent, recipients);
        }
    }
}
