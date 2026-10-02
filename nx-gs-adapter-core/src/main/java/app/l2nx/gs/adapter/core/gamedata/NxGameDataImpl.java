package app.l2nx.gs.adapter.core.gamedata;

import app.l2nx.gs.adapter.api.spi.capability.NxGameData;
import app.l2nx.gs.adapter.api.spi.capability.NxGameDataTrigger;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Trigger registry bound by gd-sync on connect; survives reconnect (IO executor swapped per handshake).
 * Triggers run on the IO executor so the host game thread never blocks on Kafka; Throwable is caught to protect it.
 */
public final class NxGameDataImpl implements NxGameData {

    private static final NxLog log = NxLogFactory.getLogger(NxGameDataImpl.class);

    private final CopyOnWriteArrayList<NxGameDataTrigger> triggers = new CopyOnWriteArrayList<NxGameDataTrigger>();
    private final AtomicReference<Executor> ioExecutor = new AtomicReference<Executor>();

    public NxGameDataImpl() {}

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
            // No executor bound yet (pre-wired / test): run inline so the request isn't dropped
            safe.run();
            return;
        }
        try {
            io.execute(safe);
        } catch (Throwable t) {
            log.warn(
                    "NxGameData failed to dispatch snapshot trigger onto IO executor: {}",
                    t.getClass().getName(),
                    t);
        }
    }
}
