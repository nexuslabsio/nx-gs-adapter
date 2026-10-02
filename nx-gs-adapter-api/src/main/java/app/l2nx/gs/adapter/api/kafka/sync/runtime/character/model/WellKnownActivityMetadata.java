package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore;

/**
 * Canonical keys and values for {@link Activity#getMetadata() activities[].metadata}. Open set: hosts may publish
 * other keys, consumers ignore unknown ones. All values are stringified; durations are ISO-8601 ({@code "PT15M"}).
 */
public final class WellKnownActivityMetadata {

    private WellKnownActivityMetadata() {}

    public static final String ELAPSED = "elapsed";

    /**
     * Omitted when the activity has no countdown.
     */
    public static final String REMAINING = "remaining";

    /**
     * Raw-seconds spelling of {@link #ELAPSED}.
     *
     * @deprecated use {@link #ELAPSED} (ISO-8601). Removed once every host emits the ISO key -
     *     i.e. after the host's first game-server restart following the release that switched.
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

    /**
     * Catch-chance multiplier as a decimal string (e.g. {@code "0.5"}); absent when the host's penalty system is off.
     */
    public static final String PENALTY_MULTIPLIER = "penalty_multiplier";

    /**
     * One of {@link #TIER_NONE}, {@link #TIER_1}, {@link #TIER_2} (worst).
     */
    public static final String PENALTY_TIER = "penalty_tier";

    /**
     * Omitted at the worst tier (no next).
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

    public static final String TIER_NONE = "none";

    public static final String TIER_1 = "tier1";

    public static final String TIER_2 = "tier2";

    /**
     * {@link CharacterPrivateStore} constant names, as on the db-sync side; omitted when the host's taxonomy has no match.
     */
    public static final String STORE_TYPE = "store_type";
}
