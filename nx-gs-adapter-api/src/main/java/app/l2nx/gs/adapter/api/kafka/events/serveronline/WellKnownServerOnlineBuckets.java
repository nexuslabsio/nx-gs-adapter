package app.l2nx.gs.adapter.api.kafka.events.serveronline;

/**
 * Canonical bucket-key constants for the {@code buckets} map of
 * {@link ServerOnlineSnapshotEvent}. Hosts MAY use additional non-canonical
 * keys — the platform treats unknown keys as opaque strings — but populating
 * these constants when applicable lets cross-tenant dashboards aggregate the
 * same concept consistently.
 *
 * <p>Catalog curated from L2 Lineage 2 game-mechanic vocabulary covering
 * L2J / Lucera / Essence forks. Adding a new constant is a non-breaking
 * minor-version change in {@code nx-gs-adapter-api}.</p>
 *
 * <p><b>Bucket overlap is host-defined.</b> A fishing player typically counts
 * toward {@link #FISHING}, {@link #REAL}, and {@link #ONLINE} simultaneously.
 * {@link #TOTAL} is published as an explicit map entry — consumers MUST NOT
 * derive it as {@code sum(buckets)} since the buckets are not disjoint.</p>
 */
public final class WellKnownServerOnlineBuckets {

    private WellKnownServerOnlineBuckets() {
    }

    /**
     * Total player presence on the server. Includes offline-trade parked
     * players and bot-driven phantoms; this is the upper bound a host
     * tracks. Used as the operator-facing "общий онлайн" counter.
     */
    public static final String TOTAL = "total";

    /**
     * Players actively in the world — i.e. {@link #TOTAL} minus
     * {@link #OFFLINE_TRADE}. Includes phantoms.
     */
    public static final String ONLINE = "online";

    /**
     * Real (non-phantom) human players. Hosts that don't track phantoms
     * separately MAY publish {@link #REAL} equal to {@link #TOTAL}; the
     * key is still meaningful for downstream charts.
     */
    public static final String REAL = "real";

    /**
     * Players parked in offline-trade mode (private store kept open while
     * the client is disconnected). On bohpts: {@code Player.isInOfflineMode()}.
     */
    public static final String OFFLINE_TRADE = "offline_trade";

    /**
     * Players currently fishing — typically a subset of {@link #REAL}.
     * On bohpts: {@code Player.isFishing()}.
     */
    public static final String FISHING = "fishing";

    /**
     * Bot-driven / fake players. Typically equals {@link #ONLINE} minus
     * {@link #REAL}. On bohpts: {@code Player.isFakePlayer()}.
     */
    public static final String PHANTOMS = "phantoms";
}
