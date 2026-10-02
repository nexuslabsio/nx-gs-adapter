package app.l2nx.gs.adapter.api.kafka.commands.telegram;

import java.util.Objects;

/**
 * Resolved character's primary key. The platform persists a tentative binding immediately, before the user types the
 * code back into the bot.
 */
public final class TelegramCharLinkResult {

    private final Long charId;

    public TelegramCharLinkResult(Long charId) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        this.charId = charId;
    }

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
