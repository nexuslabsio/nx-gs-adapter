package app.l2nx.gs.adapter.api.kafka.commands.announcement;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to broadcast a one-shot chat
 * announcement. Server-agnostic: the platform's scheduler (or an operator's
 * "send now" action) decides *when* to fire; this command carries only the
 * final text and channel, nothing about scheduling or origin.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link AnnounceResult}{@code >}
 * — {@code ok(payload)} on a successful broadcast; common error replies:</p>
 * <ul>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing {@code text}
 *     (Gson defaults a missing wire field to {@code null}; the handler MUST
 *     null-check before applying).</li>
 *     <li>{@code INTERNAL_ERROR} — broadcast mechanism failed host-side.</li>
 * </ul>
 *
 * <p><b>Text format.</b> {@link #getText() text} is the platform's neutral
 * chat micro-format: plain text, literal {@code \n} (U+000A) as a hard line
 * break, and bare {@code http(s)://} URLs for auto-linking. It never carries
 * the bohpts-specific wire tokens {@code /n} (legacy two-character line-break
 * encoding) or {@code [=url=]} (clickable-URL wrapper) — translating the
 * neutral format into those host tokens (and back, for {@code GAME}-origin
 * rows mirrored via the db-sync stream) is entirely a host concern.</p>
 *
 * <p><b>Channel.</b> {@link #isCritical() critical} selects the broadcast
 * channel: {@code false} = normal announcement channel, {@code true} =
 * the more visible critical/alert channel. Applies to the whole message —
 * there is no per-line channel mixing.</p>
 *
 * <p><b>Scope.</b> Targets exactly one server, routed by
 * {@code Nx-Target-Server-Id}; there is no per-line or per-recipient
 * targeting on the wire.</p>
 *
 * <p><b>Idempotency.</b> NOT idempotent — re-delivery (e.g. Kafka redelivery
 * on crash recovery) re-broadcasts the message. Announcements are ephemeral
 * chat lines with no unique id to dedupe on; a duplicate broadcast is a
 * player-visible but low-severity defect, not a correctness violation.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class AnnounceNowCommand implements NxCommand<AnnounceResult> {

    private final String text;
    private final boolean critical;

    public AnnounceNowCommand(String text, boolean critical) {
        if (text == null) {
            throw new IllegalArgumentException("text is required");
        }
        this.text = text;
        this.critical = critical;
    }

    /**
     * Neutral chat micro-format: plain text + literal {@code \n} hard line
     * breaks + bare {@code http(s)://} URLs. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when missing.
     */
    public String getText() {
        return text;
    }

    /**
     * {@code true} routes the broadcast through the more visible
     * critical/alert channel; {@code false} uses the normal announcement
     * channel. Applies to the entire message.
     */
    public boolean isCritical() {
        return critical;
    }

    public Builder toBuilder() {
        return new Builder().text(text).critical(critical);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnnounceNowCommand)) return false;
        AnnounceNowCommand that = (AnnounceNowCommand) o;
        return critical == that.critical && Objects.equals(text, that.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, critical);
    }

    @Override
    public String toString() {
        return "AnnounceNowCommand[text=" + text + ", critical=" + critical + "]";
    }

    public static final class Builder {
        private String text;
        private boolean critical;

        public Builder text(String text) {
            this.text = text;
            return this;
        }

        public Builder critical(boolean critical) {
            this.critical = critical;
            return this;
        }

        public AnnounceNowCommand build() {
            return new AnnounceNowCommand(text, critical);
        }
    }
}
