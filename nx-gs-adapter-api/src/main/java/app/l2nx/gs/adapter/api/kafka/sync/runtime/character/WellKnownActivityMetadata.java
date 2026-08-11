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
 * <p><b>Durations are ISO-8601</b> ({@code "PT15M"} / {@code "PT4H59M49S"}) — the same form every
 * other duration on the platform takes, so a consumer parses one format. The superseded
 * {@code *_seconds} / {@code seconds_*} keys carried raw seconds and are deprecated below.</p>
 *
 * <h2>Common</h2>
 * <ul>
 *   <li>{@link #ELAPSED} — how long the character has been in this activity.
 *   Applicable to any timed activity, not just fishing.</li>
 *   <li>{@link #REMAINING} — how long before a timed activity expires.</li>
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
 *   <li>{@link #TIME_TO_NEXT_TIER} — further active fishing until the next
 *   penalty tier kicks in. Omitted once at the worst tier (no next).</li>
 * </ul>
 *
 * <h2>Autofarming ({@link WellKnownActivities#AUTOFARMING})</h2>
 * <ul>
 *   <li>{@link #REMAINING} — purchased auto-farm time left before it expires.
 *   Omitted when the farm is unlimited / free (no countdown).</li>
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
     * How long the character has been in this activity, ISO-8601 (any timed activity).
     */
    public static final String ELAPSED = "elapsed";

    /**
     * How long before a timed activity expires, ISO-8601 (e.g. purchased
     * auto-farm time). Omitted when the activity has no countdown.
     */
    public static final String REMAINING = "remaining";

    /**
     * Raw-seconds spelling of {@link #ELAPSED}.
     *
     * @deprecated use {@link #ELAPSED} (ISO-8601). Removed once every host emits the ISO key —
     *     for bohpts, the morning game-server restart following the core release that switched.
     */
    @Deprecated
    public static final String ELAPSED_SECONDS = "elapsed_seconds";

    /**
     * Raw-seconds spelling of {@link #REMAINING}.
     *
     * @deprecated use {@link #REMAINING} (ISO-8601). Same removal gate as {@link #ELAPSED_SECONDS}.
     */
    @Deprecated
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
     * Further active fishing until the next penalty tier, ISO-8601.
     */
    public static final String TIME_TO_NEXT_TIER = "time_to_next_tier";

    /**
     * Raw-seconds spelling of {@link #TIME_TO_NEXT_TIER}.
     *
     * @deprecated use {@link #TIME_TO_NEXT_TIER} (ISO-8601). Same removal gate as
     *     {@link #ELAPSED_SECONDS}.
     */
    @Deprecated
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
