package app.l2nx.gs.adapter.api.kafka.events.raid;

/**
 * Canonical key constants for the open {@code metadata} maps of the raid-family
 * boss DTOs — {@code RaidKillEvent} (in {@code events.raid.kill}) and
 * {@code BossRespawnEntry} (in {@code events.raid.respawn}). Both carry an open
 * string→string {@code metadata} map; hosts MAY publish arbitrary additional
 * keys and consumers treat unknown keys as opaque. Adding a constant here is a
 * non-breaking minor-version change. Mirrors the {@code WellKnown*} pattern on
 * the other event DTOs ({@code WellKnownBossStatuses}, {@code WellKnownDeathMetadata}).
 *
 * <ul>
 *   <li>{@link #DIVISION} — the boss's division grouping, a
 *   {@link WellKnownBossDivisions} value. Hosts MAY set
 *   {@code metadata["division"]} on a boss-respawn entry and / or a raid-kill
 *   event to tag which division the boss belongs to. The value is an open
 *   string with no intensity / ordering implied — consumers treat unknown
 *   division strings as opaque. Absent when the host does not classify the
 *   boss into a division.</li>
 * </ul>
 */
public final class WellKnownBossMetadata {

    private WellKnownBossMetadata() {
    }

    /** Metadata key tagging a boss's division grouping; value is a {@link WellKnownBossDivisions} string. */
    public static final String DIVISION = "division";
}
