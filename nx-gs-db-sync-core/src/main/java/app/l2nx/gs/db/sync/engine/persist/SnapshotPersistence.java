package app.l2nx.gs.db.sync.engine.persist;

import app.l2nx.gs.db.sync.engine.SnapshotStore;

import java.io.Closeable;

/**
 * Snapshot durability boundary.
 *
 * <p>Lifecycle, driven by {@code CdcEngine}:</p>
 * <ol>
 *     <li>{@link #load} once on start, before any tick is scheduled.</li>
 *     <li>{@link #checkpoint} after every per-entity cycle, on that entity's
 *     pool thread — distinct entities may call concurrently from different
 *     CDC pool workers, so implementations must be thread-safe on the
 *     cross-entity bookkeeping. Implementations are expected to throttle
 *     internally.</li>
 *     <li>{@link #flushAll} once on stop, before the in-memory snapshot is
 *     cleared. Bypasses throttle so the freshest state always hits disk.</li>
 *     <li>{@link #close} once on stop, after {@code flushAll}, to release
 *     any lock / file handle.</li>
 * </ol>
 */
public interface SnapshotPersistence extends Closeable {

    void load(SnapshotStore target);

    void checkpoint(String entityName, SnapshotStore source);

    void flushAll(SnapshotStore source);

    @Override
    void close();
}
