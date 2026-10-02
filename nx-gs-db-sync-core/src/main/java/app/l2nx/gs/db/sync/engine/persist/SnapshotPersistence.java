package app.l2nx.gs.db.sync.engine.persist;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import java.io.Closeable;

/**
 * Durability boundary driven by {@code CdcEngine}: {@link #load} on start, {@link #checkpoint} after each entity cycle (concurrent across
 * entities, so thread-safe and self-throttling), then {@link #flushAll} (bypasses throttle) and {@link #close} on stop.
 */
public interface SnapshotPersistence extends Closeable {

    void load(SnapshotStore target);

    void checkpoint(String entityName, SnapshotStore source);

    void flushAll(SnapshotStore source);

    @Override
    void close();
}
