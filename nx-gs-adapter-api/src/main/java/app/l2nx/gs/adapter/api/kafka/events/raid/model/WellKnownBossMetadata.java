package app.l2nx.gs.adapter.api.kafka.events.raid.model;

/**
 * Canonical keys for the open {@code metadata} maps of {@code RaidKillEvent} and {@code BossRespawnEntry}.
 * Hosts MAY publish other keys; consumers treat unknown keys as opaque.
 */
public final class WellKnownBossMetadata {

    private WellKnownBossMetadata() {}

    /** Boss division grouping, a {@link WellKnownBossDivisions} string; absent when the host does not classify. */
    public static final String DIVISION = "division";
}
