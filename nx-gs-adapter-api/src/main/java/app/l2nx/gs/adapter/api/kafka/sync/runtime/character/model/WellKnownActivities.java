package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.CharacterRuntimeDto;

/**
 * Canonical {@code type} values for {@link Activity#getType()} in {@link CharacterRuntimeDto#getActivities()}.
 * Open vocabulary: the activity set varies per core, so hosts may emit any lower_snake_case key and consumers
 * fall back to the raw string. Extras ride the metadata map, see {@link WellKnownActivityMetadata}.
 */
public final class WellKnownActivities {

    private WellKnownActivities() {}

    public static final String FISHING = "fishing";

    public static final String READING = "reading";

    /**
     * Server-side auto-hunt. Remaining purchased time rides {@link WellKnownActivityMetadata#REMAINING}.
     */
    public static final String AUTOFARMING = "autofarming";

    /**
     * Server-managed cycle-macro session, distinct from {@link #AUTOFARMING}. Carries
     * {@link WellKnownActivityMetadata#ELAPSED} and, on quota-limited builds, {@link WellKnownActivityMetadata#REMAINING}.
     */
    public static final String AUTOMACRO = "automacro";

    /**
     * Private store with the client attached; mode in {@link WellKnownActivityMetadata#STORE_TYPE}.
     */
    public static final String TRADE = "trade";

    /**
     * Private store kept running after the player disconnected. Presence is reported separately
     * ({@code online = false}); mode in {@link WellKnownActivityMetadata#STORE_TYPE}.
     */
    public static final String OFFLINE_TRADE = "offline_trade";
}
