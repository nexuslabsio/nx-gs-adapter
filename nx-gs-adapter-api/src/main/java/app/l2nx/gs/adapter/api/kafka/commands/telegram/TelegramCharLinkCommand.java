package app.l2nx.gs.adapter.api.kafka.commands.telegram;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Telegram-to-character linking handshake: the game-server resolves the character, mails the verification code to its
 * in-game mailbox and replies with the resolved {@code charId}.
 * Errors: {@code NOT_FOUND} (no such char on the account), {@code FORBIDDEN} (host policy refuses, e.g. banned),
 * {@code RATE_LIMITED} (too many attempts for this character), {@code VALIDATION_FAILED} (missing field),
 * {@code INTERNAL_ERROR} (mail composition/delivery failed).
 * Record key is the producer's choice; meant to be {@link #getTelegramUserId() telegramUserId} for sequential attempts
 * per user (the adapter never reads it).
 * Delivery is at-most-once, but a caller re-issuing after a reply timeout sends the mail twice (duplicate mails).
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

    /** Handler MUST emit {@code VALIDATION_FAILED} on missing wire data. */
    public String getAccountName() {
        return accountName;
    }

    /** Handler resolves {@code (accountName, charName)} to a {@code charId}, else {@code NOT_FOUND}. */
    public String getCharName() {
        return charName;
    }

    /** Free-form; the handler does NOT validate format. */
    public String getConfirmationCode() {
        return confirmationCode;
    }

    /** Not used for game-side identity resolution; the host MAY include it in the mail body for audit. */
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
