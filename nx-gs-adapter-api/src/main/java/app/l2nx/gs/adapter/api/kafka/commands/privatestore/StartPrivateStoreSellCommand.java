package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.OwnerVerified;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.model.SellLine;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Opens a regular private store on the character's game thread. Errors: {@code NOT_FOUND} (char not online),
 * {@code VALIDATION_FAILED} (lines missing/empty or malformed), {@code INVALID_STATE} (cannot open a store now).
 * Individual lines may be dropped while the store still opens (see {@link StartPrivateStoreResult#getDropped()}).
 */
public final class StartPrivateStoreSellCommand implements OwnerVerified, NxCommand<StartPrivateStoreResult> {

    private final int charId;
    private final @Nullable String title;
    private final List<SellLine> lines;
    private final boolean ownerVerified;

    public StartPrivateStoreSellCommand(
            int charId, @Nullable String title, List<SellLine> lines, boolean ownerVerified) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("lines is required and must be non-empty");
        }
        this.charId = charId;
        this.title = title;
        this.lines = PrivateStoreLists.freeze(lines);
        this.ownerVerified = ownerVerified;
    }

    public int getCharId() {
        return charId;
    }

    /** {@code null} falls back to the host's default banner. */
    public @Nullable String getTitle() {
        return title;
    }

    public List<SellLine> getLines() {
        return lines;
    }

    @Override
    public boolean isOwnerVerified() {
        return ownerVerified;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).title(title).lines(lines).ownerVerified(ownerVerified);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StartPrivateStoreSellCommand)) return false;
        StartPrivateStoreSellCommand that = (StartPrivateStoreSellCommand) o;
        return charId == that.charId
                && ownerVerified == that.ownerVerified
                && Objects.equals(title, that.title)
                && Objects.equals(lines, that.lines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, title, lines, ownerVerified);
    }

    @Override
    public String toString() {
        return "StartPrivateStoreSellCommand[charId=" + charId + ", title=" + title + ", lines=" + lines
                + ", ownerVerified=" + ownerVerified + "]";
    }

    public static final class Builder {
        private int charId;
        private @Nullable String title;
        private @Nullable List<SellLine> lines;
        private boolean ownerVerified;

        public Builder charId(int charId) {
            this.charId = charId;
            return this;
        }

        public Builder title(@Nullable String title) {
            this.title = title;
            return this;
        }

        public Builder lines(List<SellLine> lines) {
            this.lines = lines;
            return this;
        }

        public Builder ownerVerified(boolean ownerVerified) {
            this.ownerVerified = ownerVerified;
            return this;
        }

        public StartPrivateStoreSellCommand build() {
            return new StartPrivateStoreSellCommand(charId, title, lines, ownerVerified);
        }
    }
}
