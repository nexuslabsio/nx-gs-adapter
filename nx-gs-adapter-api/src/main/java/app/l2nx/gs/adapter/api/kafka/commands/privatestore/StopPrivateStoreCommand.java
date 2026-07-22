package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to close whatever private
 * store a character currently has open (sell, package-sell, or buy).
 * Executed by the host's private-store subsystem on the character's game
 * thread.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link StopPrivateStoreResult}{@code >}
 * — {@code success(payload)} echoes which store type was closed. Common error
 * replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — {@code charId} does not exist / is not online
 *     on this server.</li>
 *     <li>{@code INVALID_STATE} — the character has no private store open.</li>
 * </ul>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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
