package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.provider.ItemTemplateProvider;

/**
 * Obtained via {@link ConnectContext#gameData()}. The {@code gd-sync} module publishes an initial snapshot
 * itself; the host calls {@link #publishSnapshot()} to republish static templates on demand (e.g. after a
 * datapack reload). Adapter-core owns this facade so it survives reconnect; the module re-registers its
 * trigger on each handshake.
 */
public interface NxGameData {

    /**
     * Non-blocking and idempotent: schedules a full snapshot (UPSERTs from each {@link ItemTemplateProvider}
     * plus a terminal complete marker) on an adapter daemon thread via every registered trigger.
     */
    void publishSnapshot();

    /** Module-side hook; triggers must not block or throw. */
    void registerSnapshotTrigger(NxGameDataTrigger trigger);
}
