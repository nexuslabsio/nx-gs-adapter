package app.l2nx.gs.adapter.api.spi;

import java.util.Collection;

/**
 * Module-side SAM registered with {@link NxSync#registerTrigger}. Invoked
 * on the caller's thread (Kafka consumer or game thread) — implementations
 * MUST NOT block beyond a queue submission and MUST NOT throw. PKs MAY be
 * used as a targeted-scan hint or ignored in favor of a full cycle.
 */
@FunctionalInterface
public interface NxSyncTrigger {

    void onRequest(Collection<Long> pks);
}
