package app.l2nx.gs.adapter.api.kafka.commands.gd;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;

/**
 * Re-publishes a full snapshot of every registered game-data entity, the same burst as on connect / datapack reload.
 * Always full snapshot; partition key {@code null}.
 *
 * <p>Ack at schedule time; completion shows as per-entity {@code SNAPSHOT_COMPLETE} markers in nx-gamedata.
 * {@code UNAVAILABLE} when gd-sync is inactive (disabled, failed, no provider, no gd topics from {@code /connect}).</p>
 */
public final class GdResyncCommand implements NxCommand<GdResyncResult> {

    public GdResyncCommand() {}

    public Builder toBuilder() {
        return new Builder();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof GdResyncCommand;
    }

    @Override
    public int hashCode() {
        return GdResyncCommand.class.hashCode();
    }

    @Override
    public String toString() {
        return "GdResyncCommand[]";
    }

    public static final class Builder {

        public GdResyncCommand build() {
            return new GdResyncCommand();
        }
    }
}
