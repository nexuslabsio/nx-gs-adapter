package app.l2nx.gs.adapter.api.kafka.ops;

/**
 * Per-entity operational state surfaced by the CDC engine on every cycle inside
 * {@link EntityStats#getState()}. Wire shape: enum-name string ({@code "HEALTHY"} /
 * {@code "DEGRADED"}). Platform-side consumers SHOULD treat unknown values as
 * {@code UNKNOWN} for forward-compat.
 */
public enum EntityState {

    /**
     * Last cycle completed without error — Phase 1 + Phase 2 + publish all clean.
     */
    HEALTHY,

    /**
     * Last cycle threw, hit a query timeout, or had a Kafka publish failure. The
     * engine keeps ticking; the snapshot for failed PKs stays unadvanced and is
     * replayed on the next cycle.
     */
    DEGRADED
}
