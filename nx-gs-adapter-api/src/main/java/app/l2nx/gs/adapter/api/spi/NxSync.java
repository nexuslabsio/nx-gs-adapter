package app.l2nx.gs.adapter.api.spi;

import java.util.Collection;

/**
 * Out-of-band sync trigger — host calls {@link #requestNow} after mutating
 * entity state (item transfer, account change, mail attach, …) so the next
 * sync pass for the affected entity runs immediately instead of waiting up
 * to the next scheduled tick. Acquired via {@link ConnectContext#sync()} or
 * {@link CommandContext#sync()}.
 *
 * <p>{@link #requestNow} MUST NOT block, throw, or propagate failure.
 * Unknown entity (no trigger registered) is a no-op + DEBUG log.</p>
 *
 * <p>{@link #registerTrigger} is the module-side hook bound by sync
 * modules during their connect lifecycle — last-write-wins on the
 * entity name.</p>
 */
public interface NxSync {

    void requestNow(String entityName, long pk);

    void requestNow(String entityName, Collection<Long> pks);

    void registerTrigger(String entityName, NxSyncTrigger trigger);
}
