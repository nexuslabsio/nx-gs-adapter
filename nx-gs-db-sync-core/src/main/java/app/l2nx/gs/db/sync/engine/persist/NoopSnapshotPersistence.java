package app.l2nx.gs.db.sync.engine.persist;

import app.l2nx.gs.db.sync.engine.SnapshotStore;

/**
 * No-op {@link SnapshotPersistence} — used by tests that don't need disk I/O.
 */
public final class NoopSnapshotPersistence implements SnapshotPersistence {

    public static final NoopSnapshotPersistence INSTANCE = new NoopSnapshotPersistence();

    private NoopSnapshotPersistence() {
    }

    @Override
    public void load(SnapshotStore target) {
    }

    @Override
    public void checkpoint(String entityName, SnapshotStore source) {
    }

    @Override
    public void flushAll(SnapshotStore source) {
    }

    @Override
    public void close() {
    }
}
