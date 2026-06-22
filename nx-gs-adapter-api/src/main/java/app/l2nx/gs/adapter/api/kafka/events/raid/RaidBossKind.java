package app.l2nx.gs.adapter.api.kafka.events.raid;

/**
 * Coarse classification of the raid boss this event describes. Shared by the
 * raid-kill facts and the boss-respawn snapshot — both pipelines MUST derive
 * the kind through the same cascade so a boss reports identically on either
 * stream.
 *
 * <p>The host applies the detection cascade in this order — first match
 * wins:</p>
 *
 * <ol>
 *     <li>Boss belongs to a tracked division (the host's configured raid-boss
 *     divisions — e.g. pivowar / lowwar / midwar / bigwar) → {@link #EPIC}. Division
 *     membership is checked FIRST and outranks the instance check, so a war boss
 *     spawned into a reflection / instanced arena is still {@link #EPIC}.</li>
 *     <li>Otherwise, killed / spawned inside a reflection / instance zone
 *     ({@code Attackable.getReflection().isDefault() == false}) → {@link #INSTANCE_BOSS}.</li>
 *     <li>Otherwise — any other open-world {@code isRaid() && !isRaidMinion()}
 *     attackable → {@link #RAID}.</li>
 * </ol>
 *
 * <p>The enum is intentionally coarse — finer taxonomy (per-division grouping,
 * daily-zone vs. story instance) is consumer-side metadata derived from
 * {@code bossNpcId}.</p>
 */
public enum RaidBossKind {

    /**
     * Regular raid boss spawned in the open world. Fallback when the boss is
     * neither inside an instance ({@link #INSTANCE_BOSS}) nor a member of a
     * tracked division ({@link #EPIC}). Typical examples: world raid bosses on
     * respawn timers that no division tracks.
     */
    RAID,

    /**
     * Boss that belongs to a tracked raid-boss division — the host's configured
     * divisions (e.g. pivowar / lowwar / midwar / bigwar). Detection:
     * membership in one of those divisions, NOT the host instance class. A boss
     * promoted into / dropped from a division changes kind purely through
     * configuration.
     */
    EPIC,

    /**
     * Boss inside an instance / reflection that is NOT in a tracked division —
     * Freya, Tiat, daily-zone bosses. Detection: {@code getReflection().isDefault()
     * == false} AND no division membership. A boss that sits in a tracked division
     * is classified {@link #EPIC} even when it dies in an instance — division
     * membership is checked first in the cascade.
     */
    INSTANCE_BOSS,
}
