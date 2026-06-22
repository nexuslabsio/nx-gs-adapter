package app.l2nx.gs.adapter.core.sync;

import app.l2nx.gs.adapter.api.spi.NxSync;
import app.l2nx.gs.adapter.api.spi.NxSyncResyncHandler;
import app.l2nx.gs.adapter.api.spi.NxSyncTrigger;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Session-scoped {@link NxSync} façade — per-entity {@link NxSyncTrigger}
 * registry plus a single {@link NxSyncResyncHandler}, populated by sync modules
 * during {@code onConnect}. Survives reconnect; triggers and the resync handler
 * re-register on each handshake.
 *
 * <p>Catches {@code Throwable} from triggers / the resync handler to keep the
 * game thread safe; unknown entity / no handler is DEBUG-logged (host may call
 * unconditionally).</p>
 */
public final class NxSyncImpl implements NxSync {

    private static final NxLog log = NxLogFactory.getLogger(NxSyncImpl.class);

    private final ConcurrentHashMap<String, NxSyncTrigger> triggers = new ConcurrentHashMap<String, NxSyncTrigger>();
    private final AtomicReference<NxSyncResyncHandler> resyncHandler = new AtomicReference<NxSyncResyncHandler>();

    public NxSyncImpl() {}

    @Override
    public void requestNow(String entityName, long pk) {
        requestNow(entityName, Collections.singletonList(pk));
    }

    @Override
    public void requestNow(String entityName, Collection<Long> pks) {
        NxSyncTrigger trigger = triggers.get(entityName);
        if (trigger == null) {
            log.debug("NxSync.requestNow({}, {}) — no trigger registered, dropping", entityName, pks.size());
            return;
        }
        try {
            trigger.onRequest(pks);
        } catch (Throwable t) {
            log.warn(
                    "NxSyncTrigger.onRequest for entity {} threw {}: {}",
                    entityName,
                    t.getClass().getName(),
                    t.getMessage(),
                    t);
        }
    }

    @Override
    public void requestResync(String entityName, Collection<Long> pks, boolean cascade) {
        if (pks == null || pks.isEmpty()) {
            return;
        }
        NxSyncResyncHandler handler = resyncHandler.get();
        if (handler == null) {
            log.debug(
                    "NxSync.requestResync({}, {}, cascade={}) — no resync handler registered, dropping",
                    entityName,
                    pks.size(),
                    cascade);
            return;
        }
        try {
            handler.onResync(entityName, pks, cascade);
        } catch (Throwable t) {
            log.warn(
                    "NxSyncResyncHandler.onResync for entity {} threw {}: {}",
                    entityName,
                    t.getClass().getName(),
                    t.getMessage(),
                    t);
        }
    }

    @Override
    public void registerTrigger(String entityName, NxSyncTrigger trigger) {
        NxSyncTrigger previous = triggers.put(entityName, trigger);
        if (previous != null) {
            log.warn("NxSync trigger for entity {} replaced (last-write-wins)", entityName);
        } else {
            log.info("NxSync trigger registered for entity {}", entityName);
        }
    }

    @Override
    public void registerResyncHandler(NxSyncResyncHandler handler) {
        NxSyncResyncHandler previous = resyncHandler.getAndSet(handler);
        if (previous != null) {
            log.warn("NxSync resync handler replaced (last-write-wins)");
        } else {
            log.info("NxSync resync handler registered");
        }
    }

    public void clearTriggers() {
        int count = triggers.size();
        triggers.clear();
        resyncHandler.set(null);
        if (count > 0) {
            log.info("NxSync trigger registry cleared ({} entries)", count);
        }
    }
}
