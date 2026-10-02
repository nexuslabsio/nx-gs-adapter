package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.spi.model.EntityMapping;
import java.util.Collection;

/**
 * Handles {@link NxSync#requestResync(String, Collection, boolean)} (cascade children via
 * {@link EntityMapping#parentRefs()}). Runs on the caller's thread: must not block beyond a queue submission or
 * throw, and must hop cascade JDBC and snapshot perturbation onto an adapter pool. Emits no completion event.
 */
@FunctionalInterface
public interface NxSyncResyncHandler {

    void onResync(String entityName, Collection<Long> pks, boolean cascade);
}
