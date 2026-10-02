package app.l2nx.gs.adapter.api.spi.capability;

import java.util.Collection;

/**
 * Runs on the caller's thread (Kafka consumer or game thread): must not block beyond a queue submission or throw.
 * {@code pks} are a targeted-scan hint and may be ignored in favor of a full cycle.
 */
@FunctionalInterface
public interface NxSyncTrigger {

    void onRequest(Collection<Long> pks);
}
