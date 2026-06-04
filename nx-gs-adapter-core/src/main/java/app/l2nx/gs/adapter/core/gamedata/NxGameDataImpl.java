package app.l2nx.gs.adapter.core.gamedata;

import app.l2nx.gs.adapter.api.spi.NxGameData;
import app.l2nx.gs.adapter.api.spi.NxGameDataTrigger;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Session-scoped {@link NxGameData} façade — a registry of snapshot triggers
 * bound by the {@code gd-sync} module during {@code onConnect}. Survives
 * reconnect: the captured reference keeps working while the underlying IO
 * executor is swapped on each handshake, and triggers re-register on every
 * fresh connect.
 *
 * <p>{@link #publishSnapshot()} fans out to every registered trigger, each run
 * on the adapter IO executor so the caller (host game thread) never blocks on
 * Kafka latency. Catches {@code Throwable} from triggers to keep the host
 * thread safe.</p>
 */
public final class NxGameDataImpl implements NxGameData {

    private static final NxLog log = NxLogFactory.getLogger(NxGameDataImpl.class);

    private final CopyOnWriteArrayList<NxGameDataTrigger> triggers = new CopyOnWriteArrayList<NxGameDataTrigger>();
    private final AtomicReference<Executor> ioExecutor = new AtomicReference<Executor>();

    public NxGameDataImpl() {
    }

    /**
     * Swap the IO executor used to run triggers. Called by adapter-core on every
     * handshake so the stable façade always dispatches onto the live pool.
     */
    public void bindExecutor(Executor io) {
        ioExecutor.set(io);
    }

    @Override
    public void publishSnapshot() {
        if (triggers.isEmpty()) {
            log.debug("NxGameData.publishSnapshot — no snapshot triggers registered, dropping");
            return;
        }
        Executor io = ioExecutor.get();
        for (NxGameDataTrigger trigger : triggers) {
            dispatch(io, trigger);
        }
    }

    @Override
    public void registerSnapshotTrigger(NxGameDataTrigger trigger) {
        if (trigger == null) {
            return;
        }
        triggers.add(trigger);
        log.info("NxGameData snapshot trigger registered ({} total)", triggers.size());
    }

    /**
     * Drop all registered triggers. Called on reconnect so the gd-sync module
     * re-registers cleanly instead of stacking duplicate triggers.
     */
    public void clearTriggers() {
        int count = triggers.size();
        triggers.clear();
        if (count > 0) {
            log.info("NxGameData trigger registry cleared ({} entries)", count);
        }
    }

    private void dispatch(Executor io, NxGameDataTrigger trigger) {
        Runnable safe = () -> {
            try {
                trigger.run();
            } catch (Throwable t) {
                log.warn("NxGameDataTrigger.run threw {}: {}", t.getClass().getName(), t.getMessage(), t);
            }
        };
        if (io == null) {
            // No executor bound yet (pre-wired / test context) — run inline so the
            // request is not silently dropped.
            safe.run();
            return;
        }
        try {
            io.execute(safe);
        } catch (Throwable t) {
            log.warn("NxGameData failed to dispatch snapshot trigger onto IO executor: {}",
                    t.getClass().getName(), t);
        }
    }
}
