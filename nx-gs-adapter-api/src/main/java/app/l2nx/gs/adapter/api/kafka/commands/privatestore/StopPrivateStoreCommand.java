package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.OwnerVerified;
import java.util.Objects;

/**
 * Closes whatever private store the character has open, on the game thread. Errors: {@code NOT_FOUND} (char not
 * online), {@code INVALID_STATE} (no store open).
 */
public final class StopPrivateStoreCommand implements OwnerVerified, NxCommand<StopPrivateStoreResult> {

    private final int charId;
    private final boolean ownerVerified;

    public StopPrivateStoreCommand(int charId, boolean ownerVerified) {
        this.charId = charId;
        this.ownerVerified = ownerVerified;
    }

    public int getCharId() {
        return charId;
    }

    @Override
    public boolean isOwnerVerified() {
        return ownerVerified;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).ownerVerified(ownerVerified);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StopPrivateStoreCommand)) return false;
        StopPrivateStoreCommand that = (StopPrivateStoreCommand) o;
        return charId == that.charId && ownerVerified == that.ownerVerified;
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, ownerVerified);
    }

    @Override
    public String toString() {
        return "StopPrivateStoreCommand[charId=" + charId + ", ownerVerified=" + ownerVerified + "]";
    }

    public static final class Builder {
        private int charId;
        private boolean ownerVerified;

        public Builder charId(int charId) {
            this.charId = charId;
            return this;
        }

        public Builder ownerVerified(boolean ownerVerified) {
            this.ownerVerified = ownerVerified;
            return this;
        }

        public StopPrivateStoreCommand build() {
            return new StopPrivateStoreCommand(charId, ownerVerified);
        }
    }
}
