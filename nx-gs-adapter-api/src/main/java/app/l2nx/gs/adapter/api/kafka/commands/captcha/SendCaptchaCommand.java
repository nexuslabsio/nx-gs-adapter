package app.l2nx.gs.adapter.api.kafka.commands.captcha;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Inbound command starting the host's human-verification check on an online character.
 *
 * <p>Reply: {@link CommandResult}{@code <}{@link SendCaptchaResult}{@code >}, deferred - it
 * arrives when the check ends, minutes after the command. A check that cannot start replies at once:
 * {@code VALIDATION_FAILED}, {@code NOT_FOUND} (no such character or not in the world),
 * {@code INVALID_STATE} with {@code reason} {@code ALREADY_ACTIVE} / {@code SERVER_PLAYS_CHARACTER},
 * {@code RATE_LIMITED}, {@code UNAVAILABLE}.</p>
 *
 * <p>Not idempotent and not deduped: one open check per character, so a second delivery is
 * rejected as {@code ALREADY_ACTIVE}.</p>
 */
public final class SendCaptchaCommand implements NxCommand<SendCaptchaResult> {

    private final Long characterId;
    private final @Nullable String issuedBy;
    private final @Nullable String staffNotes;

    public SendCaptchaCommand(Long characterId, @Nullable String issuedBy, @Nullable String staffNotes) {
        if (characterId == null) {
            throw new IllegalArgumentException("characterId is required");
        }
        this.characterId = characterId;
        this.issuedBy = issuedBy;
        this.staffNotes = staffNotes;
    }

    public Long getCharacterId() {
        return characterId;
    }

    /** Staff login or service label of whoever asked; echoed in the result so consumers can attribute checks they did not start. */
    public @Nullable String getIssuedBy() {
        return issuedBy;
    }

    /** Staff-only note: never shown in-game, logged by the host and surfaced on the platform command audit. */
    public @Nullable String getStaffNotes() {
        return staffNotes;
    }

    public Builder toBuilder() {
        return new Builder().characterId(characterId).issuedBy(issuedBy).staffNotes(staffNotes);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SendCaptchaCommand)) return false;
        SendCaptchaCommand that = (SendCaptchaCommand) o;
        return Objects.equals(characterId, that.characterId)
                && Objects.equals(issuedBy, that.issuedBy)
                && Objects.equals(staffNotes, that.staffNotes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(characterId, issuedBy, staffNotes);
    }

    @Override
    public String toString() {
        return "SendCaptchaCommand[characterId=" + characterId + ", issuedBy=" + issuedBy + ", staffNotes=" + staffNotes
                + "]";
    }

    public static final class Builder {
        private Long characterId;
        private @Nullable String issuedBy;
        private @Nullable String staffNotes;

        public Builder characterId(Long characterId) {
            this.characterId = characterId;
            return this;
        }

        public Builder issuedBy(@Nullable String issuedBy) {
            this.issuedBy = issuedBy;
            return this;
        }

        public Builder staffNotes(@Nullable String staffNotes) {
            this.staffNotes = staffNotes;
            return this;
        }

        public SendCaptchaCommand build() {
            return new SendCaptchaCommand(characterId, issuedBy, staffNotes);
        }
    }
}
