package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Closes whatever private store the character has open; executed on the character's game thread.
 * Reply: {@code success(StopPrivateStoreResult)}; errors: {@code NOT_FOUND} (char not online),
 * {@code INVALID_STATE} (no store open).
 */
public final class StopPrivateStoreCommand implements NxCommand<StopPrivateStoreResult> {

    private final int charId;

    public StopPrivateStoreCommand(int charId) {
        this.charId = charId;
    }

    public int getCharId() {
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
        if (!(o instanceof StopPrivateStoreCommand)) return false;
        StopPrivateStoreCommand that = (StopPrivateStoreCommand) o;
        return charId == that.charId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId);
    }

    @Override
    public String toString() {
        return "StopPrivateStoreCommand[charId=" + charId + "]";
    }

    public static final class Builder {
        private int charId;

        public Builder charId(int charId) {
            this.charId = charId;
            return this;
        }

        public StopPrivateStoreCommand build() {
            return new StopPrivateStoreCommand(charId);
        }
    }
}
