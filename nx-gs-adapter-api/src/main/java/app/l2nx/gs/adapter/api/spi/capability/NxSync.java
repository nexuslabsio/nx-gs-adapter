package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.model.EntityMapping;
import java.util.Collection;

/**
 * Asks sync modules to run an immediate pass for an entity instead of waiting for the next tick; obtained via
 * {@link ConnectContext#sync()} or {@link CommandContext#sync()}. {@code requestNow} never blocks or throws;
 * an entity with no registered trigger is a DEBUG no-op. {@code registerTrigger} is module-side, last write wins per entity.
 */
public interface NxSync {

    void requestNow(String entityName, long pk);

    void requestNow(String entityName, Collection<Long> pks);

    /**
     * Unlike {@link #requestNow} (CRC diff, republishes only changed rows), guarantees republication of the
     * named rows on the next immediate cycle even if content is byte-identical. With {@code cascade}, child rows
     * declared via {@link EntityMapping#parentRefs()} are republished too.
     *
     * <p>Fire-and-forget: never blocks or throws, work runs on adapter pools. Internal per-command resync:
     * emits no {@code ResyncCompletedEvent}. Unknown entity or null/empty {@code pks} is a no-op.</p>
     *
     * @param entityName as declared by {@link EntityMapping#entityName()}
     */
    void requestResync(String entityName, Collection<Long> pks, boolean cascade);

    void registerTrigger(String entityName, NxSyncTrigger trigger);

    /** Module-side hook routing {@link #requestResync}; last registration wins, dropped silently when absent. */
    void registerResyncHandler(NxSyncResyncHandler handler);
}
