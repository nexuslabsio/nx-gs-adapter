package app.l2nx.gs.adapter.api.kafka.ops.model;

/**
 * Per-entity CDC state, serialized as the enum name.
 * Consumers SHOULD treat unknown values as {@code UNKNOWN}.
 */
public enum EntityState {
    HEALTHY,

    /** Last cycle failed; unadvanced snapshots of failed PKs are replayed next cycle. */
    DEGRADED
}
