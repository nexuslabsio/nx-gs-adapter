package app.l2nx.gs.adapter.api.kafka.commands.announcement;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to delete one row from its
 * native {@code auto_announcements} table (or equivalent). Used for two
 * platform flows: an operator deleting a {@code GAME}-origin row directly
 * ("delete in game"), and the {@code GAME}→{@code L2NX} transfer flow, where
 * the platform first creates its own copy of the announcement and then issues
 * this command to remove the now-redundant source row so it is not
 * re-ingested by the next db-sync cycle.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <Void>}
 * — {@code ok()} on a successful delete (no typed payload; the caller
 * already knows the {@code gameId} it asked to delete). Common error
 * replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — no row with the given {@code gameId} exists.</li>
 *     <li>{@code INTERNAL_ERROR} — delete failed host-side.</li>
 * </ul>
 *
 * <p><b>Identity.</b> {@link #getGameId() gameId} is the host's native
 * {@code auto_announcements} row id — the same value surfaced as
 * {@code AutoAnnouncementDbDto.id} on the db-sync mirror stream. REQUIRED.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class DeleteAutoAnnouncementCommand implements NxCommand<Void> {

    private final long gameId;

    public DeleteAutoAnnouncementCommand(long gameId) {
        this.gameId = gameId;
    }

    /**
     * Id of the {@code auto_announcements} row to delete on the host.
     * REQUIRED.
     */
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
