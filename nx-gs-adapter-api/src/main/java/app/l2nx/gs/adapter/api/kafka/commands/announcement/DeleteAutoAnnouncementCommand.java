package app.l2nx.gs.adapter.api.kafka.commands.announcement;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Deletes one row from the host's native {@code auto_announcements} table. Used for operator "delete in game"
 * and the GAME->L2NX transfer, where the source row is removed so the next db-sync does not re-ingest it.
 *
 * <p>Reply: {@code CommandResult<Void>}; {@code NOT_FOUND} if no such row, {@code INTERNAL_ERROR} on host-side failure.</p>
 */
public final class DeleteAutoAnnouncementCommand implements NxCommand<Void> {

    private final long gameId;

    public DeleteAutoAnnouncementCommand(long gameId) {
        this.gameId = gameId;
    }

    /** Host's native row id; same value as {@code AutoAnnouncementDbDto.id} on the db-sync stream. */
    public long getGameId() {
        return gameId;
    }

    public Builder toBuilder() {
        return new Builder().gameId(gameId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteAutoAnnouncementCommand)) return false;
        DeleteAutoAnnouncementCommand that = (DeleteAutoAnnouncementCommand) o;
        return gameId == that.gameId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gameId);
    }

    @Override
    public String toString() {
        return "DeleteAutoAnnouncementCommand[gameId=" + gameId + "]";
    }

    public static final class Builder {
        private long gameId;

        public Builder gameId(long gameId) {
            this.gameId = gameId;
            return this;
        }

        public DeleteAutoAnnouncementCommand build() {
            return new DeleteAutoAnnouncementCommand(gameId);
        }
    }
}
