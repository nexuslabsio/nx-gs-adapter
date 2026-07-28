package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore;

/**
 * Canonical keys (and a few canonical values) for the open
 * {@link Activity#getMetadata() activities[].metadata} map. Mirrors the
 * {@code WellKnown*Metadata} constants on the discrete event DTOs
 * ({@code WellKnownPresenceMetadata}, {@code WellKnownGameEventMetadata}).
 *
 * <p>All metadata values are <b>stringified</b> — numbers and enums are carried
 * as their string form (the platform stores the whole {@code activities}
 * array as JSONB and the dashboard parses what it needs). The key set is <b>open and
 * non-exhaustive</b>: a host MAY publish arbitrary keys for its own activities
 * without an API release, and consumers ignore keys they do not understand.
 * Adding a constant here is a non-breaking minor-version change.</p>
 *
 * <h2>Common</h2>
 * <ul>
 *   <li>{@link #ELAPSED_SECONDS} — how long the character has been in this
 *   activity, in seconds. Applicable to any timed activity, not just fishing.</li>
 * </ul>
 *
 * <h2>Fishing ({@link WellKnownActivities#FISHING})</h2>
 * Penalty model: catch-chance degrades the longer a character fishes (active
 * time, paused while not fishing). The host maps its own tier thresholds /
 * multipliers onto these keys; absent when the host's penalty system is disabled.
 * <ul>
 *   <li>{@link #PENALTY_MULTIPLIER} — current catch-chance multiplier as a
 *   decimal string (e.g. {@code "1.0"} / {@code "0.5"} / {@code "0.1"}).</li>
 *   <li>{@link #PENALTY_TIER} — current penalty tier; canonical values
 *   {@link #TIER_NONE} / {@link #TIER_1} / {@link #TIER_2}.</li>
 *   <li>{@link #SECONDS_TO_NEXT_TIER} — seconds of further active fishing until
 *   the next penalty tier kicks in. Omitted once at the worst tier (no next).</li>
 * </ul>
 *
 * <h2>Autofarming ({@link WellKnownActivities#AUTOFARMING})</h2>
 * <ul>
 *   <li>{@link #SECONDS_REMAINING} — seconds of purchased auto-farm time left
 *   before it expires. Omitted when the farm is unlimited / free (no countdown).</li>
 * </ul>
 *
 * <h2>Trading ({@link WellKnownActivities#TRADE} / {@link WellKnownActivities#OFFLINE_TRADE})</h2>
 * <ul>
 *   <li>{@link #STORE_TYPE} — which kind of private store is open.</li>
 * </ul>
 */
public final class WellKnownActivityMetadata {

    private WellKnownActivityMetadata() {}

    // ── Common ──────────────────────────────────────────────────────────────

    /**
     * Seconds the character has been in this activity (any timed activity).
     */
    public static final String ELAPSED_SECONDS = "elapsed_seconds";

    /**
     * Seconds remaining before a timed activity expires (e.g. purchased
     * auto-farm time). Omitted when the activity has no countdown.
     */
    public static final String SECONDS_REMAINING = "seconds_remaining";

    // ── Fishing ─────────────────────────────────────────────────────────────

    /**
     * Current catch-chance multiplier as a decimal string (e.g. {@code "0.5"}).
     */
    public static final String PENALTY_MULTIPLIER = "penalty_multiplier";

    /**
     * Current penalty tier — {@link #TIER_NONE} / {@link #TIER_1} / {@link #TIER_2}.
     */
    public static final String PENALTY_TIER = "penalty_tier";

    /**
     * Seconds of further active fishing until the next penalty tier.
     */
    public static final String SECONDS_TO_NEXT_TIER = "seconds_to_next_tier";

    /**
     * {@link #PENALTY_TIER} value — no penalty (base catch chance).
     */
    public static final String TIER_NONE = "none";

    /**
     * {@link #PENALTY_TIER} value — first penalty tier.
     */
    public static final String TIER_1 = "tier1";

    /**
     * {@link #PENALTY_TIER} value — second (worst) penalty tier.
     */
    public static final String TIER_2 = "tier2";

    // ── Trading ─────────────────────────────────────────────────────────────

    /**
     * Which kind of private store is open. Values are
     * {@link CharacterPrivateStore} constant names ({@code SELL} / {@code BUY} /
     * {@code CRAFT} / {@code PACKAGE_SELL}) — the same vocabulary the db-sync
     * side uses, so a consumer resolves one enum for both channels. Hosts whose
     * store taxonomy has no matching constant omit the key rather than invent a
     * value.
     */
    public static final String STORE_TYPE = "store_type";
}
