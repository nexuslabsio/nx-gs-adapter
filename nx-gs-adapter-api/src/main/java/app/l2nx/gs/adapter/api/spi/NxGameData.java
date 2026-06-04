package app.l2nx.gs.adapter.api.spi;

/**
 * Game-data sync capability, acquired via {@link ConnectContext#gameData()}. Lets
 * the host trigger a fresh full-snapshot publish of static game-data templates
 * (item-templates today; skills / npc later) onto the {@code gd} sync stream.
 *
 * <p>The {@code gd-sync} module publishes an initial snapshot automatically once
 * connected. The host calls {@link #publishSnapshot()} to re-publish on demand —
 * e.g. after an in-game datapack reload — so the platform picks up changes without
 * a restart. The call is non-blocking: it schedules the snapshot on an adapter
 * daemon thread and returns immediately. Always non-null — a context wired without
 * a gd-sync runtime hands back a no-op that drops the request.</p>
 *
 * <p>Mirrors the {@link NxSync} trigger pattern: the {@code gd-sync} module
 * registers its snapshot runner via {@link #registerSnapshotTrigger(NxGameDataTrigger)}
 * during its connect lifecycle; {@link #publishSnapshot()} fans out to every
 * registered trigger. Adapter-core owns the façade so it survives reconnect — the
 * module re-registers its trigger on each handshake.</p>
 */
public interface NxGameData {

    /**
     * Schedule a fresh full snapshot of every registered game-data entity (pulls
     * each {@link ItemTemplateProvider}, publishes UPSERTs + a terminal complete
     * marker). Idempotent and safe to call repeatedly. Fans out to every
     * {@link NxGameDataTrigger} registered via
     * {@link #registerSnapshotTrigger(NxGameDataTrigger)}.
     */
    void publishSnapshot();

    /**
     * Module-side hook bound by the {@code gd-sync} module during its connect
     * lifecycle. Each registered trigger runs a fresh full-snapshot publish when
     * the host calls {@link #publishSnapshot()}. Triggers MUST NOT block, throw,
     * or propagate failure — the façade invokes them defensively.
     */
    void registerSnapshotTrigger(NxGameDataTrigger trigger);
}
