package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

/**
 * Canonical boss values of {@link NpcTemplate#getType()}; other types are host-defined and opaque to consumers.
 * Which bosses are {@link #EPIC_BOSS} is decided by the host.
 */
public final class WellKnownNpcTypes {

    private WellKnownNpcTypes() {}

    public static final String RAID_BOSS = "RAID_BOSS";

    public static final String EPIC_BOSS = "EPIC_BOSS";
}
