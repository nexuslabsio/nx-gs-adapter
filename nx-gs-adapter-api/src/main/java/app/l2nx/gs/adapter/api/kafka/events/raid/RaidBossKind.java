package app.l2nx.gs.adapter.api.kafka.events.raid;

/**
 * Coarse classification of the raid boss this kill event describes.
 *
 * <p>The host SHOULD apply the detection cascade in this order — first match
 * wins — when populating {@code RaidKillEvent.getBossKind()}:</p>
 *
 * <ol>
 *     <li>Killed inside a reflection / instance zone
 *     ({@code Attackable.getReflection().isDefault() == false}) → {@link #INSTANCE_BOSS}.</li>
 *     <li>Boss instance type is {@code GrandBossInstance} (or fork equivalent) → {@link #GRAND_BOSS}.</li>
 *     <li>Otherwise — any other {@code isRaid() && !isRaidMinion()} attackable
 *     in open world → {@link #RAID}.</li>
 * </ol>
 *
 * <p>The enum is intentionally coarse — finer taxonomy (e.g. EPIC subgrouping
 * of grand bosses, daily-zone vs. story instance) is consumer-side metadata
 * derived from {@code bossNpcId}.</p>
 */
public enum RaidBossKind {

    /**
     * Regular raid boss spawned in the open world. Generic {@code isRaid()}
     * fallback when neither {@link #INSTANCE_BOSS} nor {@link #GRAND_BOSS}
     * applies. Typical examples: world raid bosses on respawn timers.
     */
    RAID,

    /**
     * Grand / epic boss — Antharas, Valakas, Baium, Beleth, Frintezza,
     * Lindvior, etc. Long fixed respawn cadence, world events, public
     * announcements. Detection: host instance is {@code GrandBossInstance}
     * (or fork-equivalent subclass).
     */
    GRAND_BOSS,

    /**
     * Boss inside an instance / reflection — Freya, Tiat, daily-zone bosses.
     * Detection: {@code getReflection().isDefault() == false}. Instance bosses
     * skip the {@link #GRAND_BOSS} branch even when their template is a
     * {@code GrandBossInstance} subclass, because the killer-pool is bounded
     * to the instance participants rather than the entire server.
     */
    INSTANCE_BOSS,
}
