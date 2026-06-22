package app.l2nx.gs.adapter.api.kafka.events.raid;

/**
 * Coarse classification of the raid boss an event describes — a shared
 * vocabulary only. The host integration decides which value applies to a given
 * boss per its own rules; the adapter and platform attach no detection logic
 * and infer nothing from it. The same value is reused by the raid-kill facts
 * and the boss-respawn snapshot.
 *
 * <p>Intentionally coarse — any finer taxonomy is consumer-side metadata
 * derived from {@code bossNpcId}.</p>
 */
public enum RaidBossKind {

    /** A regular raid boss. */
    RAID,

    /** An epic / grand boss — which bosses qualify is decided by the host. */
    EPIC,

    /** A boss fought inside an instanced encounter. */
    INSTANCE_BOSS,
}
