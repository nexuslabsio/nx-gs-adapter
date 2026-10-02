package app.l2nx.gs.adapter.api.kafka.commands.chat;

import java.util.Objects;

/** Telemetry only: the platform stores the message from its echo event, not from this reply. */
public final class SendChatMessageResult {

    private final int linesSent;
    private final int recipients;

    public SendChatMessageResult(int linesSent, int recipients) {
        this.linesSent = linesSent;
        this.recipients = recipients;
    }

    /** Non-empty lines after the host splits the text on {@code \n}. */
    public int getLinesSent() {
        return linesSent;
    }

    /** Best-effort; hosts that do not track it MAY report {@code 0}, which does NOT mean failure. */
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
