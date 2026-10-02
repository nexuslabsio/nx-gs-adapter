package app.l2nx.gs.adapter.api.kafka.commands.character;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Disconnects an online character, either dropping it to the login screen or closing the client.
 *
 * <p>Reply: {@code CommandResult<KickCharacterResult>}. Errors: {@code NOT_FOUND} (no such character),
 * {@code INVALID_STATE} (not online), {@code VALIDATION_FAILED} ({@code charId} missing / out of range),
 * {@code INTERNAL_ERROR}.</p>
 *
 * <p>Routed by {@code charId}. Not idempotent (a re-delivery after the player relogged kicks again), so hosts
 * dedupe on the correlation id.</p>
 */
public final class KickCharacterCommand implements NxCommand<KickCharacterResult> {

    private final Long charId;
    private final boolean closeClient;
    private final @Nullable String staffNotes;

    public KickCharacterCommand(Long charId, boolean closeClient, @Nullable String staffNotes) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        this.charId = charId;
        this.closeClient = closeClient;
        this.staffNotes = staffNotes;
    }

    public Long getCharId() {
        return charId;
    }

    /** {@code true} closes the game client; {@code false} drops the player to the login screen. */
    public boolean isCloseClient() {
        return closeClient;
    }

    /** Staff-only note: never shown in-game, logged by the host and surfaced on the platform command audit. */
    public @Nullable String getStaffNotes() {
        return staffNotes;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).closeClient(closeClient).staffNotes(staffNotes);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KickCharacterCommand)) return false;
        KickCharacterCommand that = (KickCharacterCommand) o;
        return closeClient == that.closeClient
                && Objects.equals(charId, that.charId)
                && Objects.equals(staffNotes, that.staffNotes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, closeClient, staffNotes);
    }

    @Override
    public String toString() {
        return "KickCharacterCommand[charId=" + charId + ", closeClient=" + closeClient + ", staffNotes=" + staffNotes
                + "]";
    }

    public static final class Builder {
        private Long charId;
        private boolean closeClient;
        private @Nullable String staffNotes;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder closeClient(boolean closeClient) {
            this.closeClient = closeClient;
            return this;
        }

        public Builder staffNotes(@Nullable String staffNotes) {
            this.staffNotes = staffNotes;
            return this;
        }

        public KickCharacterCommand build() {
            return new KickCharacterCommand(charId, closeClient, staffNotes);
        }
    }
}
