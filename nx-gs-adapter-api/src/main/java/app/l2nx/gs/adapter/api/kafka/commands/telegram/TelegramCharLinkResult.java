package app.l2nx.gs.adapter.api.kafka.commands.telegram;

import java.util.Objects;

/**
 * Success payload of {@link TelegramCharLinkCommand}: the resolved
 * character's primary key. The platform persists a tentative binding
 * between {@link TelegramCharLinkCommand#getTelegramUserId() telegramUserId}
 * and this {@code charId} immediately, before the user types the code back
 * into the bot.
 *
 * <p>Java 8 POJO; final field; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter name.</p>
 */
public final class TelegramCharLinkResult {

    private final Long charId;

    public TelegramCharLinkResult(Long charId) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        this.charId = charId;
    }

    /**
     * Primary key of the character on which {@code (accountName, charName)}
     * resolved. Always non-null on a success envelope.
     */
    public Long getCharId() {
        return charId;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TelegramCharLinkResult)) return false;
        TelegramCharLinkResult that = (TelegramCharLinkResult) o;
        return Objects.equals(charId, that.charId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId);
    }

    @Override
    public String toString() {
        return "TelegramCharLinkResult[charId=" + charId + "]";
    }

    public static final class Builder {
        private Long charId;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public TelegramCharLinkResult build() {
            return new TelegramCharLinkResult(charId);
        }
    }
}
