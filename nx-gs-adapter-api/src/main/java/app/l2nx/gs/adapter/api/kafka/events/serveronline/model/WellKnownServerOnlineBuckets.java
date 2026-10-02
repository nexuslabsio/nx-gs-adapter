package app.l2nx.gs.adapter.api.kafka.events.serveronline.model;

import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;

/**
 * Canonical keys of the {@code buckets} map of {@link ServerOnlineSnapshotEvent}. The map is open: hosts
 * MAY add keys and the platform treats unknown ones as opaque.
 *
 * <p>{@link #TOTAL} and {@link #UNIQUE} are required; the rest are optional and consumers MUST tolerate
 * their absence. Soft invariant: {@code total >= unique + offline_trade}. Informational only; consumers
 * MUST NOT reject violating snapshots (tick-walk races cause drift).</p>
 */
public final class WellKnownServerOnlineBuckets {

    private WellKnownServerOnlineBuckets() {}

    /** Every character the host tracks, including offline-trade sessions and bot-driven phantoms. */
    public static final String TOTAL = "total";

    /** Distinct active human players, deduplicated by a host-defined identity tuple (e.g. HWID + IP). */
    public static final String UNIQUE = "unique";

    /** Players parked in offline-trade mode (private store open, client disconnected). */
    public static final String OFFLINE_TRADE = "offline_trade";

    /** Characters currently fishing; whether offline-trade fishers count is host-defined. */
    public static final String FISHING = "fishing";
}
