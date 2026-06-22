package app.l2nx.gs.adapter.api.kafka.commands.gd;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;

/**
 * Inbound command instructing the gd-sync module to re-publish a full
 * snapshot of every registered game-data entity (itemtemplate, npctemplate,
 * skill, recipe, armorset, soulcrystal, class, instance). Pure adapter
 * operation — no host game code involved; the snapshot is the same burst the
 * module fires on connect / host datapack-reload.
 *
 * <p>No fields — granularity is always full snapshot (no per-entity
 * selection). Partition key {@code null} (round-robin).</p>
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link GdResyncResult}{@code >}
 * — an <b>ack</b> returned after the snapshot is scheduled; it does NOT wait
 * for the publish to finish. Completion is observable on the platform via the
 * per-entity {@code SNAPSHOT_COMPLETE} markers in nx-gamedata, not a separate
 * completion event. Common error reply:</p>
 * <ul>
 *     <li>{@code UNAVAILABLE} — the gd-sync module is not active (disabled /
 *     failed / no provider on the classpath / no gd topics from
 *     {@code /connect}).</li>
 * </ul>
 *
 * <p>Java 8 POJO; final class; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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
