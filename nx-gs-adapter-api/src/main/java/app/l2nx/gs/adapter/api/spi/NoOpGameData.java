package app.l2nx.gs.adapter.api.spi;

/**
 * Silent-drop fallback used when no gd-sync runtime is wired (tests,
 * pre-bootstrap contexts).
 */
final class NoOpGameData implements NxGameData {

    static final NoOpGameData INSTANCE = new NoOpGameData();

    private NoOpGameData() {}

    @Override
    public void publishSnapshot() {}

    @Override
    public void registerSnapshotTrigger(NxGameDataTrigger trigger) {}
}
