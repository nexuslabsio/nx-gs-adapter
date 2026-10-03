package app.l2nx.gs.adapter.api.kafka.events.raid.model;

/**
 * Canonical keys for the open {@code metadata} maps of {@code RaidKillEvent} and {@code BossRespawnEntry}.
 * Hosts MAY publish other keys; consumers treat unknown keys as opaque.
 */
public final class WellKnownBossMetadata {

    private WellKnownBossMetadata() {}

    /**
     * Boss division grouping, a {@link WellKnownBossDivisions} string; absent when the host does not classify.
     *
     * @deprecated the platform no longer reads division from the wire.
     */
    @Deprecated
    // TODO: remove after the deprecation cycle, once no host emits and no consumer reads the division key
    public static final String DIVISION = "division";
}
