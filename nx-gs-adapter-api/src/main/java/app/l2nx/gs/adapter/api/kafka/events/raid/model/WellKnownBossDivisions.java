package app.l2nx.gs.adapter.api.kafka.events.raid.model;

/**
 * Canonical values for the {@code division} metadata key ({@link WellKnownBossMetadata#DIVISION}).
 * Open string, no ordering implied; hosts MAY emit other values and consumers treat unknown ones as opaque.
 *
 * @deprecated the platform no longer reads division from the wire; grouping is platform-side.
 */
@Deprecated
// TODO: remove after the deprecation cycle, once no host emits and no consumer reads the division key
public final class WellKnownBossDivisions {

    private WellKnownBossDivisions() {}

    public static final String PIVOWAR = "pivowar";

    public static final String LOWWAR = "lowwar";

    public static final String MIDWAR = "midwar";

    public static final String BIGWAR = "bigwar";
}
