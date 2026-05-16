package app.l2nx.gs.adapter.api.spi;

import java.util.Collection;

/**
 * Silent-drop fallback used when no sync runtime is wired (tests,
 * pre-bootstrap contexts).
 */
final class NoOpSync implements NxSync {

    static final NoOpSync INSTANCE = new NoOpSync();

    private NoOpSync() {
    }

    @Override
    public void requestNow(String entityName, long pk) {
    }

    @Override
    public void requestNow(String entityName, Collection<Long> pks) {
    }

    @Override
    public void registerTrigger(String entityName, NxSyncTrigger trigger) {
    }
}
