package app.l2nx.gs.adapter.api.spi;

import java.util.Collection;

/**
 * Module-side SAM registered with {@link NxSync#registerResyncHandler}. Handles
 * {@link NxSync#requestResync(String, Collection, boolean)} by force-republishing
 * the given PKs of {@code entityName} on the sync module's next immediate cycle
 * (and, when {@code cascade} is set, the child rows declared via
 * {@link EntityMapping#parentRefs()}).
 *
 * <p>Invoked on the caller's thread (game thread or Kafka consumer thread) — the
 * implementation MUST NOT block beyond a queue submission and MUST NOT throw. Any
 * cascade-resolution IO (JDBC) and snapshot perturbation MUST be hopped onto an
 * adapter pool, never run on the caller. Unlike the admin force-resync command
 * handlers, this path emits NO completion event.</p>
 */
@FunctionalInterface
public interface NxSyncResyncHandler {

    void onResync(String entityName, Collection<Long> pks, boolean cascade);
}
