package app.l2nx.gs.adapter.api.kafka.events.serveronline;

/**
 * Canonical bucket-key constants for the {@code buckets} map of
 * {@link ServerOnlineSnapshotEvent}. The canonical set is intentionally
 * narrow — these are the keys cross-tenant dashboards aggregate on. Hosts
 * MAY publish arbitrary additional non-canonical keys (open map); the
 * platform treats unknown keys as opaque strings.
 *
 * <p>Catalog curated from L2 Lineage 2 game-mechanic vocabulary covering
 * L2J / Lucera / Essence forks. Adding a new constant is a non-breaking
 * minor-version change in {@code nx-gs-adapter-api}.</p>
 *
 * <h2>Publication contract</h2>
 *
 * <p><b>Required</b> — every snapshot MUST carry these keys:</p>
 * <ul>
 *     <li>{@link #TOTAL} — full character presence (everything the host
 *         tracks, including offline-trade and bot-driven phantoms).</li>
 *     <li>{@link #UNIQUE} — distinct active human players, deduplicated by
 *         a host-defined identity tuple (e.g. HWID + IP on bohpts).</li>
 * </ul>
 *
 * <p><b>Optional canonical</b> — host SHOULD publish when the concept
 * applies; consumers MUST tolerate absence:</p>
 * <ul>
 *     <li>{@link #OFFLINE_TRADE} — parked private-store sessions.</li>
 *     <li>{@link #FISHING} — characters currently fishing.</li>
 * </ul>
 *
 * <p>When both {@code total} and {@code unique} are published the host
 * SHOULD respect the soft invariant {@code total >= unique + offline_trade}
 * (a single human account can hold one active character plus an
 * offline-trade alt; phantoms inflate {@code total} but not {@code unique}).
 * The invariant is informational — consumers MUST NOT reject snapshots
 * that violate it, since transient race conditions during the tick walk
 * can produce minor drift.</p>
 */
public final class WellKnownServerOnlineBuckets {

    private WellKnownServerOnlineBuckets() {}

    /**
     * Required. Total character presence on the server — every entity the
     * host iterates over, including offline-trade parked sessions and
     * bot-driven phantoms. This is the upper bound a host tracks and
     * matches the operator-facing "общий онлайн" counter.
     */
    public static final String TOTAL = "total";

    /**
     * Required. Distinct active human players. Hosts deduplicate by an
     * identity tuple of their choosing — on bohpts this is
     * {@code (HWID, IP)} among {@code !isInOfflineMode() && !isFakePlayer()}
     * players, with rows whose HWID or IP is unavailable skipped. Other
     * forks MAY use {@code account_id}, {@code (account, IP)}, or any
     * host-meaningful tuple; the wire concept is "how many real humans
     * are actively playing right now."
     */
    public static final String UNIQUE = "unique";

    /**
     * Optional canonical. Players parked in offline-trade mode (private
     * store kept open while the client is disconnected). On bohpts:
     * {@code Player.isInOfflineMode()}.
     */
    public static final String OFFLINE_TRADE = "offline_trade";

    /**
     * Optional canonical. Characters currently fishing. Host-defined
     * whether offline-trade fishers count — on bohpts every
     * {@code Player.isFishing()} character is counted regardless of
     * offline-trade state.
     */
    public static final String FISHING = "fishing";
}
