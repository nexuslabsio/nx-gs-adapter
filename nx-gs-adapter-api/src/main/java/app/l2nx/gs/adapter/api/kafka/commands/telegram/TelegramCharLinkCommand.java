package app.l2nx.gs.adapter.api.kafka.commands.telegram;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command driving the Telegram ↔ character linking handshake.
 * Initiated by a Telegram bot user who supplied an in-game account name
 * and character name; the platform forwards the request as this command,
 * the game-server resolves the character, mails the verification code
 * to the character's in-game mailbox, and replies with the resolved
 * {@code charId} so the platform can persist the binding state. The
 * player completes the loop by typing the code back into the bot.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link TelegramCharLinkResult}{@code >}
 * — {@code success(payload)} carries the resolved {@code charId} of the
 * character that received the code; common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — no character with the given {@code charName}
 *     exists on the given {@code accountName}.</li>
 *     <li>{@code FORBIDDEN} — character exists but the host's policy
 *     refuses to send the code (banned account, restricted character, …).</li>
 *     <li>{@code RATE_LIMITED} — too many link attempts for this character
 *     in the recent window; user must retry later.</li>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing a required field
 *     (any of {@code accountName}, {@code charName}, {@code confirmationCode},
 *     {@code telegramUserId}).</li>
 *     <li>{@code INTERNAL_ERROR} — unexpected error during mail composition
 *     or delivery.</li>
 * </ul>
 *
 * <p><b>Required fields.</b> All four fields are semantically REQUIRED.
 * The constructor enforces non-null via {@link IllegalArgumentException}
 * for programmatic construction. Wire-path Gson bypasses the constructor —
 * handler-side null-checking is the wire-validation gate.</p>
 *
 * <p><b>Partitioning.</b> The record key is the producer's choice (see
 * {@link app.l2nx.gs.adapter.api.rest.MessagingTopics#getCommandsTopic()}); for this command it is
 * meant to be {@link #getTelegramUserId() telegramUserId}, keeping link attempts from one Telegram
 * user sequential. The adapter never reads the key.</p>
 *
 * <p><b>Re-issue safety.</b> Delivery is at-most-once (see
 * {@link app.l2nx.gs.adapter.api.spi.CommandHandler}); what repeats is a caller re-issuing after a
 * reply timeout, which the handler cannot tell from a fresh request: the verification mail is sent
 * twice — a player-facing defect (duplicate mails in the inbox).</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class TelegramCharLinkCommand implements NxCommand<TelegramCharLinkResult> {

    private final String accountName;
    private final String charName;
    private final String confirmationCode;
    private final Long telegramUserId;

    public TelegramCharLinkCommand(String accountName, String charName, String confirmationCode, Long telegramUserId) {
        if (accountName == null) {
            throw new IllegalArgumentException("accountName is required");
        }
        if (charName == null) {
            throw new IllegalArgumentException("charName is required");
        }
        if (confirmationCode == null) {
            throw new IllegalArgumentException("confirmationCode is required");
        }
        if (telegramUserId == null) {
            throw new IllegalArgumentException("telegramUserId is required");
        }
        this.accountName = accountName;
        this.charName = charName;
        this.confirmationCode = confirmationCode;
        this.telegramUserId = telegramUserId;
    }

    /**
     * Game-side login account name on which to look up {@link #getCharName()
     * charName}. REQUIRED. Handler MUST emit {@code VALIDATION_FAILED} on
     * missing wire data.
     */
    public String getAccountName() {
        return accountName;
    }

    /**
     * Character name to bind to the Telegram user. REQUIRED. Handler MUST
     * resolve {@code (accountName, charName)} to a {@code charId} and
     * reply {@code NOT_FOUND} if the pair does not match a known
     * character.
     */
    public String getCharName() {
        return charName;
    }

    /**
     * Platform-issued verification code to mail to the resolved character's
     * in-game mailbox. REQUIRED. Free-form short string (typically 6–8
     * digits); the handler does NOT validate format.
     */
    public String getConfirmationCode() {
        return confirmationCode;
    }

    /**
     * Telegram user id requesting the link. REQUIRED. Carried for partitioning
     * and so the host MAY include it in the verification mail body for
     * audit clarity; not used for game-side identity resolution.
     */
    public Long getTelegramUserId() {
        return telegramUserId;
    }

    public Builder toBuilder() {
        return new Builder()
                .accountName(accountName)
                .charName(charName)
                .confirmationCode(confirmationCode)
                .telegramUserId(telegramUserId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TelegramCharLinkCommand)) return false;
        TelegramCharLinkCommand that = (TelegramCharLinkCommand) o;
        return Objects.equals(accountName, that.accountName)
                && Objects.equals(charName, that.charName)
                && Objects.equals(confirmationCode, that.confirmationCode)
                && Objects.equals(telegramUserId, that.telegramUserId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountName, charName, confirmationCode, telegramUserId);
    }

    @Override
    public String toString() {
        return "TelegramCharLinkCommand[accountName=" + accountName
                + ", charName=" + charName
                + ", confirmationCode=" + confirmationCode
                + ", telegramUserId=" + telegramUserId + "]";
    }

    public static final class Builder {
        private String accountName;
        private String charName;
        private String confirmationCode;
        private Long telegramUserId;

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder charName(String charName) {
            this.charName = charName;
            return this;
        }

        public Builder confirmationCode(String confirmationCode) {
            this.confirmationCode = confirmationCode;
            return this;
        }

        public Builder telegramUserId(Long telegramUserId) {
            this.telegramUserId = telegramUserId;
            return this;
        }

        public TelegramCharLinkCommand build() {
            return new TelegramCharLinkCommand(accountName, charName, confirmationCode, telegramUserId);
        }
    }
}
