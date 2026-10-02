package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.spi.capability.NxGameData;
import app.l2nx.gs.adapter.api.spi.capability.NxGameDataTrigger;

/** Fallback when no gd-sync runtime is wired; drops every call. */
final class NoOpGameData implements NxGameData {

    static final NoOpGameData INSTANCE = new NoOpGameData();

    private NoOpGameData() {}

    @Override
    public void publishSnapshot() {}

    @Override
    public void registerSnapshotTrigger(NxGameDataTrigger trigger) {}
}
